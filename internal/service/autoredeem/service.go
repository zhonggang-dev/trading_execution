package autoredeem

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/port"
)

type Params struct {
	Store            port.RedemptionStore
	Venue            port.RedemptionVenue
	Receipts         port.RedemptionReceiptSource
	Activities       port.RedeemActivitySource
	PollInterval     time.Duration
	RetryInterval    time.Duration
	AmbiguityTimeout time.Duration
	BatchSize        int
	Logger           *slog.Logger
	Now              func() time.Time
}

type Service struct {
	store            port.RedemptionStore
	venue            port.RedemptionVenue
	receipts         port.RedemptionReceiptSource
	activities       port.RedeemActivitySource
	pollInterval     time.Duration
	retryInterval    time.Duration
	ambiguityTimeout time.Duration
	batchSize        int
	logger           *slog.Logger
	now              func() time.Time
}

func New(params Params) (*Service, error) {
	if params.Store == nil || params.Venue == nil || params.Receipts == nil || params.Activities == nil {
		return nil, fmt.Errorf("auto redeem requires store, venue, receipt, and activity evidence")
	}
	if params.PollInterval < 10*time.Second || params.PollInterval > 10*time.Minute {
		return nil, fmt.Errorf("auto redeem poll interval must be between 10s and 10m")
	}
	if params.RetryInterval < 5*time.Second || params.RetryInterval > time.Hour {
		return nil, fmt.Errorf("auto redeem retry interval must be between 5s and 1h")
	}
	if params.AmbiguityTimeout < time.Minute || params.AmbiguityTimeout > 24*time.Hour {
		return nil, fmt.Errorf("auto redeem ambiguity timeout must be between 1m and 24h")
	}
	if params.BatchSize <= 0 || params.BatchSize > 100 {
		return nil, fmt.Errorf("auto redeem batch size must be between 1 and 100")
	}
	if params.Logger == nil {
		params.Logger = slog.Default()
	}
	if params.Now == nil {
		params.Now = func() time.Time { return time.Now().UTC() }
	}
	return &Service{
		store: params.Store, venue: params.Venue, receipts: params.Receipts,
		activities: params.Activities, pollInterval: params.PollInterval,
		retryInterval: params.RetryInterval, ambiguityTimeout: params.AmbiguityTimeout,
		batchSize: params.BatchSize, logger: params.Logger, now: params.Now,
	}, nil
}

func (service *Service) Sweep(ctx context.Context) error {
	if err := service.store.SyncPendingRedemptions(ctx); err != nil {
		return err
	}
	now := service.now().UTC()
	redemptions, err := service.store.ListDueRedemptions(ctx, service.batchSize, now)
	if err != nil {
		return err
	}
	var failures []error
	for _, redemption := range redemptions {
		if err := service.process(ctx, redemption, now); err != nil {
			failures = append(failures, fmt.Errorf("%s/%s: %w", redemption.ExecutionAccountID, redemption.ConditionID, err))
		}
	}
	return errors.Join(failures...)
}

func (service *Service) Run(ctx context.Context) error {
	if err := service.Sweep(ctx); err != nil {
		service.logger.Error("initial Polymarket auto redeem sweep needs attention", "error", err)
	}
	ticker := time.NewTicker(service.pollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if err := service.Sweep(ctx); err != nil && ctx.Err() == nil {
				service.logger.Error("Polymarket auto redeem sweep needs attention", "error", err)
			}
		}
	}
}

func (service *Service) process(ctx context.Context, redemption domain.Redemption, now time.Time) error {
	switch redemption.Status {
	case domain.RedemptionReady:
		return service.processReady(ctx, redemption, now)
	case domain.RedemptionApprovalSubmitting:
		return service.recoverApprovalSubmitting(ctx, redemption, now)
	case domain.RedemptionApprovalSubmitted:
		return service.processApprovalSubmitted(ctx, redemption, now)
	case domain.RedemptionRedeemSubmitting:
		return service.recoverRedeemSubmitting(ctx, redemption, now)
	case domain.RedemptionRedeemSubmitted:
		return service.processRedeemSubmitted(ctx, redemption, now)
	case domain.RedemptionConfirmed:
		return service.store.ApplyRedemption(ctx, redemption, now)
	default:
		return fmt.Errorf("unexpected active redemption status %q", redemption.Status)
	}
}

func (service *Service) processReady(ctx context.Context, redemption domain.Redemption, now time.Time) error {
	approved, err := service.venue.RedemptionApproved(ctx, redemption.WalletAddress, redemption.NegRisk)
	if err != nil {
		return service.retry(ctx, redemption, err, now)
	}
	kind := domain.RedemptionSubmissionRedeem
	if !approved {
		kind = domain.RedemptionSubmissionApproval
	}
	if err := service.store.BeginRedemptionSubmission(ctx, redemption, kind, now); err != nil {
		return service.review(ctx, redemption, "redemption safety precondition failed: "+err.Error(), now)
	}
	submitting := redemption
	submitting.SubmittingAt = timePointer(now)
	if kind == domain.RedemptionSubmissionApproval {
		submitting.Status = domain.RedemptionApprovalSubmitting
	} else {
		submitting.Status = domain.RedemptionRedeemSubmitting
	}
	var submission domain.RedemptionSubmission
	if kind == domain.RedemptionSubmissionApproval {
		submission, err = service.venue.SubmitRedemptionApproval(ctx, redemption.ExecutionAccountID, redemption.NegRisk)
	} else {
		submission, err = service.venue.SubmitRedemption(ctx, redemption.ExecutionAccountID, redemption.ConditionID, redemption.NegRisk)
	}
	if err != nil {
		// The call may have reached an RPC or relayer even when no response was
		// received. Keep *_SUBMITTING and recover evidence; never resubmit.
		return service.retry(ctx, submitting, fmt.Errorf("submission outcome is ambiguous: %w", err), now)
	}
	if submission.Provider == "" || submission.Reference == "" {
		return service.review(ctx, submitting, "venue returned an incomplete submission identity", now)
	}
	return service.store.RecordRedemptionSubmission(ctx, submitting, submission, now, now.Add(service.retryInterval))
}

func (service *Service) recoverApprovalSubmitting(ctx context.Context, redemption domain.Redemption, now time.Time) error {
	approved, err := service.venue.RedemptionApproved(ctx, redemption.WalletAddress, redemption.NegRisk)
	if err != nil {
		return service.retry(ctx, redemption, err, now)
	}
	if approved {
		return service.store.ResetRedemptionReady(ctx, redemption, now)
	}
	if expired(redemption.SubmittingAt, now, service.ambiguityTimeout) {
		return service.review(ctx, redemption, "approval submit outcome remained ambiguous and adapter approval is still absent", now)
	}
	return service.store.RetryRedemption(ctx, redemption, "waiting to recover ambiguous approval submission", now.Add(service.retryInterval))
}

func (service *Service) processApprovalSubmitted(ctx context.Context, redemption domain.Redemption, now time.Time) error {
	submission, err := service.venue.ResolveRedemptionSubmission(ctx, redemption.ExecutionAccountID, submissionFrom(redemption))
	if err != nil {
		return service.retry(ctx, redemption, err, now)
	}
	switch submission.State {
	case domain.RedemptionSubmissionFailed:
		return service.review(ctx, redemption, "redemption adapter approval failed: "+submission.FailureReason, now)
	case domain.RedemptionSubmissionConfirmed:
		approved, approvalErr := service.venue.RedemptionApproved(ctx, redemption.WalletAddress, redemption.NegRisk)
		if approvalErr != nil {
			return service.retry(ctx, redemption, approvalErr, now)
		}
		if !approved {
			return service.review(ctx, redemption, "confirmed approval transaction did not grant the exact redemption adapter", now)
		}
		return service.store.ResetRedemptionReady(ctx, redemption, now)
	default:
		return service.store.RetryRedemption(ctx, redemption, "redemption adapter approval is pending", now.Add(service.retryInterval))
	}
}

func (service *Service) recoverRedeemSubmitting(ctx context.Context, redemption domain.Redemption, now time.Time) error {
	if redemption.SubmittingAt == nil {
		return service.review(ctx, redemption, "redeem submit intent has no timestamp", now)
	}
	activities, err := service.activities.ListRedeemActivities(
		ctx, redemption.WalletAddress, redemption.ConditionID, redemption.SubmittingAt.Add(-5*time.Second),
	)
	if err != nil {
		return service.retry(ctx, redemption, err, now)
	}
	if len(activities) > 1 {
		return service.review(ctx, redemption, "multiple redemption activities match one ambiguous submit intent", now)
	}
	if len(activities) == 1 {
		return service.confirmReceipt(ctx, redemption, activities[0].TransactionHash, now)
	}
	if expired(redemption.SubmittingAt, now, service.ambiguityTimeout) {
		return service.review(ctx, redemption, "redeem submit outcome remained ambiguous and no matching activity was found", now)
	}
	return service.store.RetryRedemption(ctx, redemption, "waiting to recover ambiguous redeem submission", now.Add(service.retryInterval))
}

func (service *Service) processRedeemSubmitted(ctx context.Context, redemption domain.Redemption, now time.Time) error {
	submission, err := service.venue.ResolveRedemptionSubmission(ctx, redemption.ExecutionAccountID, submissionFrom(redemption))
	if err != nil {
		return service.retry(ctx, redemption, err, now)
	}
	if submission.State == domain.RedemptionSubmissionFailed {
		return service.review(ctx, redemption, "redemption transaction failed: "+submission.FailureReason, now)
	}
	transactionHash := submission.TransactionHash
	if transactionHash == "" {
		transactionHash = redemption.TransactionHash
	}
	if transactionHash == "" {
		return service.store.RetryRedemption(ctx, redemption, "redemption transaction hash is pending", now.Add(service.retryInterval))
	}
	if redemption.TransactionHash == "" {
		if err := service.store.RecordRedemptionTransaction(ctx, redemption, transactionHash, now.Add(service.retryInterval)); err != nil {
			return err
		}
		redemption.TransactionHash = transactionHash
	}
	return service.confirmReceipt(ctx, redemption, transactionHash, now)
}

func (service *Service) confirmReceipt(ctx context.Context, redemption domain.Redemption, transactionHash string, now time.Time) error {
	receipt, err := service.receipts.ResolveRedemptionReceipt(
		ctx, transactionHash, redemption.WalletAddress, redemption.ConditionID, redemption.NegRisk,
	)
	if err != nil {
		var permanent interface{ Permanent() bool }
		if errors.As(err, &permanent) && permanent.Permanent() {
			return service.review(ctx, redemption, err.Error(), now)
		}
		return service.retry(ctx, redemption, err, now)
	}
	return service.store.RecordRedemptionConfirmed(ctx, redemption, receipt, now)
}

func (service *Service) retry(ctx context.Context, redemption domain.Redemption, cause error, now time.Time) error {
	if err := service.store.RetryRedemption(ctx, redemption, cause.Error(), now.Add(service.retryInterval)); err != nil {
		return errors.Join(cause, err)
	}
	return cause
}

func (service *Service) review(ctx context.Context, redemption domain.Redemption, reason string, now time.Time) error {
	if reason == "" {
		reason = "redemption requires manual review"
	}
	if err := service.store.ReviewRedemption(ctx, redemption, reason, now); err != nil {
		return err
	}
	return fmt.Errorf("manual review: %s", reason)
}

func submissionFrom(redemption domain.Redemption) domain.RedemptionSubmission {
	return domain.RedemptionSubmission{
		Provider: redemption.SubmissionProvider, Reference: redemption.SubmissionReference,
		TransactionHash: redemption.TransactionHash, State: domain.RedemptionSubmissionPending,
	}
}

func expired(start *time.Time, now time.Time, timeout time.Duration) bool {
	return start == nil || !now.Before(start.Add(timeout))
}

func timePointer(value time.Time) *time.Time {
	value = value.UTC()
	return &value
}
