package baselineamend

import (
	"strings"
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
)

const goodHash = "0x" + "ab12ab12ab12ab12ab12ab12ab12ab12ab12ab12ab12ab12ab12ab12ab12ab12"

func input(kind domain.BaselineAmendmentKind, chain, managed, baseline string) Input {
	index := 1
	return Input{
		Kind: kind, ExecutionAccountID: "main", WalletAddress: "0x6c07b5e271ffa2da038ce05e498cff856bf32357",
		TokenID:     "39147524522707536456910747992621671433032838141439193264830818731544404472030",
		Identity:    Identity{ConditionID: "0xcondition", OutcomeIndex: &index, OutcomeName: "No", NegRisk: false},
		ChainShares: domain.Decimal(chain), ManagedShares: domain.Decimal(managed), BaselineRemaining: domain.Decimal(baseline),
		Block:        Block{Number: 95990000, Hash: goodHash, Confirmations: 128, Time: time.Date(2026, 10, 23, 9, 0, 0, 0, time.UTC)},
		MaxAddShares: "0.1", Actor: "operator", Reason: "reason", Note: "0xtx",
	}
}

func TestBuildResidualIsExactDifference(t *testing.T) {
	amendment, err := Build(input(domain.BaselineAmendmentAddResidual, "5.025174", "5", "0"))
	if err != nil {
		t.Fatal(err)
	}
	if amendment.Shares.String() != "0.025174" || amendment.Kind != domain.BaselineAmendmentAddResidual ||
		amendment.TokenID == "" || !strings.Contains(string(amendment.Evidence), `"chain_shares":"5.025174"`) ||
		!strings.Contains(string(amendment.Evidence), `"note":"0xtx"`) {
		t.Fatalf("amendment = %#v evidence=%s", amendment, amendment.Evidence)
	}
	again, _ := Build(input(domain.BaselineAmendmentAddResidual, "5.025174", "5", "0"))
	if again.AmendmentID != amendment.AmendmentID {
		t.Fatalf("id is not deterministic: %s vs %s", again.AmendmentID, amendment.AmendmentID)
	}
	// A residual already registered earlier is subtracted, not added twice.
	next, err := Build(input(domain.BaselineAmendmentAddResidual, "5.045174", "5", "0.025174"))
	if err != nil || next.Shares.String() != "0.02" {
		t.Fatalf("incremental residual = %#v err=%v", next, err)
	}
}

func TestBuildResidualRefusals(t *testing.T) {
	for name, in := range map[string]Input{
		"nothing to add":     input(domain.BaselineAmendmentAddResidual, "5", "5", "0"),
		"already registered": input(domain.BaselineAmendmentAddResidual, "5.025174", "5", "0.025174"),
		"chain below ledger": input(domain.BaselineAmendmentAddResidual, "4.9", "5", "0"),
		"above the maximum":  input(domain.BaselineAmendmentAddResidual, "6", "5", "0"),
	} {
		if _, err := Build(in); err == nil {
			t.Errorf("%s was accepted", name)
		}
	}
}

func TestBuildRetireRequiresBurnedToken(t *testing.T) {
	amendment, err := Build(input(domain.BaselineAmendmentRetireConsumed, "0", "0", "2.94"))
	if err != nil || amendment.Shares.String() != "2.94" || amendment.Kind != domain.BaselineAmendmentRetireConsumed {
		t.Fatalf("retire = %#v err=%v", amendment, err)
	}
	for name, in := range map[string]Input{
		"wallet still holds shares": input(domain.BaselineAmendmentRetireConsumed, "1", "0", "2.94"),
		"ledger still holds shares": input(domain.BaselineAmendmentRetireConsumed, "0", "1", "2.94"),
		"no baseline":               input(domain.BaselineAmendmentRetireConsumed, "0", "0", "0"),
	} {
		if _, err := Build(in); err == nil {
			t.Errorf("%s was accepted", name)
		}
	}
}

func TestBuildRejectsIncompleteInput(t *testing.T) {
	bad := func(mutate func(*Input)) {
		in := input(domain.BaselineAmendmentAddResidual, "5.025174", "5", "0")
		mutate(&in)
		if _, err := Build(in); err == nil {
			t.Errorf("incomplete input accepted: %#v", in)
		}
	}
	bad(func(in *Input) { in.Actor = "" })
	bad(func(in *Input) { in.Reason = " " })
	bad(func(in *Input) { in.WalletAddress = "0xABC" })
	bad(func(in *Input) { in.Identity.OutcomeIndex = nil })
	bad(func(in *Input) { in.Identity.ConditionID = "" })
	bad(func(in *Input) { in.Block.Hash = "0x12" })
	bad(func(in *Input) { in.Block.Number = 0 })
	bad(func(in *Input) { in.ChainShares = "1e3" })
	bad(func(in *Input) { in.Kind = "SOMETHING" })
}
