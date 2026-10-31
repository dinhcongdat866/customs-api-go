package shipment

import (
	"context"
	"errors"
	"testing"
)

// fakeRepo implements only what Decide touches; everything else panics.
type fakeRepo struct {
	Repo
	get   func(id string) (*Shipment, error)
	apply func(id string, d ReviewDecision, code string) (*Shipment, error)
}

func (f *fakeRepo) GetShipment(_ context.Context, id string) (*Shipment, error) { return f.get(id) }
func (f *fakeRepo) ApplyDecision(_ context.Context, id string, d ReviewDecision, code string) (*Shipment, error) {
	return f.apply(id, d, code)
}

func TestDecide_ApproveUsesProposedCode(t *testing.T) {
	proposed := "6109"
	var gotCode string
	repo := &fakeRepo{
		get: func(string) (*Shipment, error) { return &Shipment{HSCode: &proposed}, nil },
		apply: func(_ string, _ ReviewDecision, code string) (*Shipment, error) {
			gotCode = code
			return &Shipment{}, nil
		},
	}
	svc := NewService(repo, nil)
	if _, err := svc.Decide(context.Background(), "s1", ReviewDecision{Decision: DecisionApprove, Reviewer: "dat"}); err != nil {
		t.Fatal(err)
	}
	if gotCode != "6109" {
		t.Fatalf("final code = %q, want 6109", gotCode)
	}
}

func TestDecide_Validation(t *testing.T) {
	svc := NewService(&fakeRepo{}, nil)
	cases := []ReviewDecision{
		{Decision: DecisionApprove},                                   // no reviewer
		{Decision: "reject", Reviewer: "dat"},                         // unknown decision
		{Decision: DecisionOverride, Reviewer: "dat"},                 // override without code
		{Decision: DecisionOverride, Reviewer: "dat", HSCode: "61x9"}, // bad code
		{Decision: DecisionApprove, Reviewer: "dat", HSCode: "6109"},  // approve with code
	}
	for i, d := range cases {
		_, err := svc.Decide(context.Background(), "s1", d)
		var ve *ValidationError
		if !errors.As(err, &ve) {
			t.Errorf("case %d: want ValidationError, got %v", i, err)
		}
	}
}
