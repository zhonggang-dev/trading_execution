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

// NegRiskAdapterV1Address is the legacy Neg Risk Adapter. The CLOB still
// checks CTF approvals for it on negative-risk orders; it is a different
// contract from the NegRiskCtfCollateralAdapter used for redemption.
const NegRiskAdapterV1Address = "0xd91e80cf2e7be2e162c6513ced06f1dd0da35296"

// RedemptionAdapterAddress returns the collateral adapter SubmitRedemption
// calls for a market of the given type.
func RedemptionAdapterAddress(negRisk bool) string { return redemptionAdapter(negRisk) }

// eoaOperatorAllowlist is the exact set of contracts an EOA may be asked to
// approve as CTF operator through SubmitEOAOperatorApproval.
var eoaOperatorAllowlist = map[string]struct{}{
	strings.ToLower(polygonExchangeV2):        {},
	strings.ToLower(polygonNegRiskExchangeV2): {},
	NegRiskAdapterV1Address:                   {},
	standardCollateralAdapter:                 {},
	negRiskCollateralAdapter:                  {},
}

// ConditionPayout is the on-chain CTF resolution of a binary condition.
// Denominator zero means the oracle has not reported yet.
type ConditionPayout struct {
	Denominator *big.Int
	Numerators  [2]*big.Int
}

// Resolved reports whether the CTF accepts redemptions for the condition.
func (payout ConditionPayout) Resolved() bool {
	return payout.Denominator != nil && payout.Denominator.Sign() > 0
}

// AdapterPositions are the two CTF position ids the collateral adapter moves
// out of the caller's wallet when redeeming a condition, and the CTF
// collateral they are derived from (USDC.e, or the legacy neg-risk wrapped
// collateral).
type AdapterPositions struct {
	Adapter    string
	Collateral string
	// PositionIDs[0] is index set 1 (first outcome), [1] is index set 2.
	PositionIDs [2]string
}

// SimulateRedemption runs eth_call, from the wallet, of the exact adapter call
// SubmitRedemption would sign. It never signs or broadcasts anything. A nil
// error proves only that the call would not revert: the adapter redeems the
// caller's whole balance, so a zero balance also succeeds and pays nothing.
func (client *RedemptionClient) SimulateRedemption(ctx context.Context, walletAddress, conditionID string, negRisk bool) error {
	if _, ok := decodeAddress(walletAddress); !ok {
		return fmt.Errorf("redemption wallet address is invalid")
	}
	call, err := redeemPositionsCall(redemptionAdapter(negRisk), conditionID)
	if err != nil {
		return err
	}
	_, err = client.rpc(ctx, "eth_call", []any{map[string]string{
		"from": strings.ToLower(strings.TrimSpace(walletAddress)), "to": call.Target, "value": "0x0", "data": call.Data,
	}, "latest"})
	return err
}

// ConditionPayout reads payoutDenominator and, when resolved, both payout
// numerators of a binary condition from the Conditional Tokens contract.
func (client *RedemptionClient) ConditionPayout(ctx context.Context, conditionID string) (ConditionPayout, error) {
	condition, err := bytes32Word(strings.ToLower(strings.TrimSpace(conditionID)))
	if err != nil {
		return ConditionPayout{}, fmt.Errorf("condition id: %w", err)
	}
	denominator, err := client.callUint256(ctx, conditionalTokensAddress, abiSelector("payoutDenominator(bytes32)"), condition)
	if err != nil {
		return ConditionPayout{}, fmt.Errorf("read payout denominator: %w", err)
	}
	payout := ConditionPayout{Denominator: denominator, Numerators: [2]*big.Int{big.NewInt(0), big.NewInt(0)}}
	if !payout.Resolved() {
		return payout, nil
	}
	for index := range payout.Numerators {
		numerator, err := client.callUint256(ctx, conditionalTokensAddress,
			abiSelector("payoutNumerators(bytes32,uint256)"), condition, uint256Word(big.NewInt(int64(index))))
		if err != nil {
			return ConditionPayout{}, fmt.Errorf("read payout numerator %d: %w", index, err)
		}
		payout.Numerators[index] = numerator
	}
	return payout, nil
}

// CTFBalance reads ConditionalTokens.balanceOf(wallet, tokenID) in base units.
func (client *RedemptionClient) CTFBalance(ctx context.Context, walletAddress, tokenID string) (*big.Int, error) {
	wallet, err := addressWord(walletAddress)
	if err != nil {
		return nil, err
	}
	token, err := parseUint256(tokenID)
	if err != nil {
		return nil, fmt.Errorf("token id: %w", err)
	}
	return client.callUint256(ctx, conditionalTokensAddress, abiSelector("balanceOf(address,uint256)"), wallet, uint256Word(token))
}

// AdapterPositions derives, from the deployed contracts, the position ids the
// collateral adapter redeems for a condition: it reads the adapter's own
// USDCE() or WRAPPED_COLLATERAL() and asks the CTF for the collection and
// position ids. Comparing these ids with a wallet's token ids proves the
// adapter will move exactly those tokens.
func (client *RedemptionClient) AdapterPositions(ctx context.Context, conditionID string, negRisk bool) (AdapterPositions, error) {
	condition, err := bytes32Word(strings.ToLower(strings.TrimSpace(conditionID)))
	if err != nil {
		return AdapterPositions{}, fmt.Errorf("condition id: %w", err)
	}
	adapter := redemptionAdapter(negRisk)
	getter := "USDCE()"
	if negRisk {
		getter = "WRAPPED_COLLATERAL()"
	}
	collateralWord, err := client.callWord(ctx, adapter, abiSelector(getter))
	if err != nil {
		return AdapterPositions{}, fmt.Errorf("read adapter %s: %w", getter, err)
	}
	for _, value := range collateralWord[:12] {
		if value != 0 {
			return AdapterPositions{}, fmt.Errorf("adapter %s is not an address", getter)
		}
	}
	result := AdapterPositions{Adapter: adapter, Collateral: "0x" + hex.EncodeToString(collateralWord[12:])}
	for index, indexSet := range []int64{1, 2} {
		collection, err := client.callWord(ctx, conditionalTokensAddress,
			abiSelector("getCollectionId(bytes32,bytes32,uint256)"), make([]byte, 32), condition, uint256Word(big.NewInt(indexSet)))
		if err != nil {
			return AdapterPositions{}, fmt.Errorf("read collection id for index set %d: %w", indexSet, err)
		}
		position, err := client.callUint256(ctx, conditionalTokensAddress,
			abiSelector("getPositionId(address,bytes32)"), collateralWord, collection)
		if err != nil {
			return AdapterPositions{}, fmt.Errorf("read position id for index set %d: %w", indexSet, err)
		}
		result.PositionIDs[index] = position.String()
	}
	return result, nil
}

// NativeBalance reads the wallet's POL balance in wei.
func (client *RedemptionClient) NativeBalance(ctx context.Context, walletAddress string) (*big.Int, error) {
	if _, ok := decodeAddress(walletAddress); !ok {
		return nil, fmt.Errorf("wallet address is invalid")
	}
	return client.rpcBigInt(ctx, "eth_getBalance", []any{strings.ToLower(strings.TrimSpace(walletAddress)), "latest"}, "native balance")
}

// SubmitEOAOperatorApproval sends ConditionalTokens.setApprovalForAll(operator,
// true) from an EOA account, through the same simulate/estimate/sign path as
// EOA redemption. Only the fixed Polymarket exchange and adapter contracts
// are accepted as operator.
func (client *RedemptionClient) SubmitEOAOperatorApproval(
	ctx context.Context, executionAccountID, operator string,
) (domain.RedemptionSubmission, error) {
	operator = strings.ToLower(strings.TrimSpace(operator))
	if _, allowed := eoaOperatorAllowlist[operator]; !allowed {
		return domain.RedemptionSubmission{}, notSubmitted(fmt.Errorf("operator %s is not an allowed Polymarket contract", operator))
	}
	account, err := client.credentials.Account(ctx, strings.TrimSpace(executionAccountID))
	if err != nil {
		return domain.RedemptionSubmission{}, notSubmitted(fmt.Errorf("select approval account: %w", err))
	}
	if account.SignatureType != SignatureTypeEOA {
		return domain.RedemptionSubmission{}, notSubmitted(fmt.Errorf("EOA operator approval requires an EOA account"))
	}
	call, err := setApprovalForAllCall(operator)
	if err != nil {
		return domain.RedemptionSubmission{}, notSubmitted(err)
	}
	return client.submitEOACall(ctx, account, call)
}

func abiSelector(signature string) []byte { return keccak256([]byte(signature))[:4] }

// callWord performs a read-only eth_call at latest and returns one 32-byte word.
func (client *RedemptionClient) callWord(ctx context.Context, to string, selector []byte, words ...[]byte) ([]byte, error) {
	data := append(append([]byte(nil), selector...), concatWords(words...)...)
	result, err := client.rpc(ctx, "eth_call", []any{
		map[string]string{"to": strings.ToLower(to), "data": "0x" + hex.EncodeToString(data)}, "latest",
	})
	if err != nil {
		return nil, err
	}
	var encoded string
	if json.Unmarshal(result, &encoded) != nil {
		return nil, fmt.Errorf("eth_call response is invalid")
	}
	return decodeFixedHex(encoded, 32, "eth_call word")
}

func (client *RedemptionClient) callUint256(ctx context.Context, to string, selector []byte, words ...[]byte) (*big.Int, error) {
	word, err := client.callWord(ctx, to, selector, words...)
	if err != nil {
		return nil, err
	}
	return new(big.Int).SetBytes(word), nil
}
