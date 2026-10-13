package polymarket

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/big"
	"strings"

	"github.com/UniPat-AI/trading_execution/internal/domain"
)

// TradingApprovalKind selects the token standard of one trading approval.
type TradingApprovalKind string

const (
	// ApprovalPUSDAllowance is pUSD.approve(spender, 2^256-1).
	ApprovalPUSDAllowance TradingApprovalKind = "PUSD_ALLOWANCE"
	// ApprovalCTFOperator is ConditionalTokens.setApprovalForAll(operator, true).
	ApprovalCTFOperator TradingApprovalKind = "CTF_OPERATOR"
)

// TradingApproval is one approval a Deposit Wallet grants to a spender.
type TradingApproval struct {
	Kind    TradingApprovalKind `json:"kind"`
	Spender string              `json:"spender"`
}

// TradingApprovalState is the current on-chain state of one TradingApproval.
type TradingApprovalState struct {
	TradingApproval
	Satisfied bool   `json:"satisfied"`
	Current   string `json:"current"`
}

var maxUint256 = new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 256), big.NewInt(1))

// ReadTradingApprovals reads each approval's current on-chain state. Allowances
// are satisfied only at the maximum value, matching the Polymarket SDK
// ("max"); an operator approval is satisfied only when it is true.
func (client *RedemptionClient) ReadTradingApprovals(
	ctx context.Context, wallet string, approvals []TradingApproval,
) ([]TradingApprovalState, error) {
	owner, ok := decodeAddress(wallet)
	if !ok {
		return nil, fmt.Errorf("approval wallet address is invalid")
	}
	states := make([]TradingApprovalState, 0, len(approvals))
	for _, approval := range approvals {
		spender, ok := decodeAddress(approval.Spender)
		if !ok {
			return nil, fmt.Errorf("approval spender %q is invalid", approval.Spender)
		}
		var to, selector string
		switch approval.Kind {
		case ApprovalPUSDAllowance:
			to, selector = PUSDAddress, "dd62ed3e" // allowance(owner,spender)
		case ApprovalCTFOperator:
			to, selector = conditionalTokensAddress, "e985e9c5" // isApprovedForAll(owner,operator)
		default:
			return nil, fmt.Errorf("unsupported approval kind %q", approval.Kind)
		}
		data := make([]byte, 4+64)
		copy(data[:4], mustHex(selector))
		copy(data[4+12:4+32], owner)
		copy(data[4+32+12:], spender)
		result, err := client.rpc(ctx, "eth_call", []any{
			map[string]string{"to": strings.ToLower(to), "data": "0x" + hex.EncodeToString(data)}, "latest",
		})
		if err != nil {
			return nil, fmt.Errorf("read %s for %s: %w", approval.Kind, approval.Spender, err)
		}
		var encoded string
		if json.Unmarshal(result, &encoded) != nil {
			return nil, fmt.Errorf("%s response is invalid", approval.Kind)
		}
		decoded, err := decodeFixedHex(encoded, 32, string(approval.Kind))
		if err != nil {
			return nil, err
		}
		value := new(big.Int).SetBytes(decoded)
		state := TradingApprovalState{TradingApproval: approval, Current: value.String()}
		if approval.Kind == ApprovalPUSDAllowance {
			state.Satisfied = value.Cmp(maxUint256) == 0
		} else {
			state.Satisfied = value.Cmp(big.NewInt(1)) == 0
		}
		states = append(states, state)
	}
	return states, nil
}

// SubmitTradingApprovals submits the approvals as one ordered Deposit Wallet
// batch through the Polymarket relayer (the signer authorizes the whole batch;
// the relayer pays gas). Only Deposit Wallet accounts are accepted.
func (client *RedemptionClient) SubmitTradingApprovals(
	ctx context.Context, executionAccountID string, approvals []TradingApproval, metadata string,
) (domain.RedemptionSubmission, error) {
	account, err := client.credentials.Account(ctx, strings.TrimSpace(executionAccountID))
	if err != nil {
		return domain.RedemptionSubmission{}, fmt.Errorf("select approval account: %w", err)
	}
	if account.SignatureType != SignatureTypePolyEIP1271 {
		return domain.RedemptionSubmission{}, fmt.Errorf("trading approvals are submitted only for Deposit Wallet accounts")
	}
	if len(approvals) == 0 {
		return domain.RedemptionSubmission{}, fmt.Errorf("no approvals to submit")
	}
	calls := make([]redemptionCall, 0, len(approvals))
	for _, approval := range approvals {
		call, err := tradingApprovalCall(approval)
		if err != nil {
			return domain.RedemptionSubmission{}, err
		}
		calls = append(calls, call)
	}
	return client.dispatch(ctx, executionAccountID, calls, metadata)
}

func tradingApprovalCall(approval TradingApproval) (redemptionCall, error) {
	switch approval.Kind {
	case ApprovalCTFOperator:
		return setApprovalForAllCall(approval.Spender)
	case ApprovalPUSDAllowance:
		spender, err := addressWord(approval.Spender)
		if err != nil {
			return redemptionCall{}, err
		}
		data := append([]byte{0x09, 0x5e, 0xa7, 0xb3}, spender...) // approve(address,uint256)
		data = append(data, uint256Word(maxUint256)...)
		return redemptionCall{Target: strings.ToLower(PUSDAddress), Value: "0", Data: "0x" + hex.EncodeToString(data)}, nil
	default:
		return redemptionCall{}, fmt.Errorf("unsupported approval kind %q", approval.Kind)
	}
}
