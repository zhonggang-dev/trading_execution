package port

import (
	"context"
	"sync"
)

// ReservationSequence orders the asset-reservation step of one batch of order
// submissions. Delivery may run the steps before reservation (risk check, live
// market validation) and after it (signing, placement) concurrently, but the
// reservation itself must happen in batch order: when the wallet cannot fund
// every order, the orders that get the money are the ones the strategy ranked
// first, exactly as in the sequential path.
//
// Turn(i) may reserve only after Turn(i-1) has been released. A turn must be
// released on every exit path; Release is idempotent so callers can both call
// it right after Reserve and defer it as a safety net.
type ReservationSequence struct {
	done []chan struct{}
	once []sync.Once
}

// NewReservationSequence returns a sequence for count ordered turns.
func NewReservationSequence(count int) *ReservationSequence {
	sequence := &ReservationSequence{done: make([]chan struct{}, count), once: make([]sync.Once, count)}
	for index := range sequence.done {
		sequence.done[index] = make(chan struct{})
	}
	return sequence
}

// Turn returns the turn at the zero-based position index. Every handle for the
// same position shares one release, so Release is safe from any of them.
func (sequence *ReservationSequence) Turn(index int) ReservationTurn {
	turn := ReservationTurn{sequence: sequence, index: index}
	if index > 0 {
		turn.previous = sequence.done[index-1]
	}
	return turn
}

// ReservationTurn is one position in a ReservationSequence.
type ReservationTurn struct {
	sequence *ReservationSequence
	index    int
	previous <-chan struct{}
}

// Wait blocks until the previous turn has been released or ctx ends. The first
// turn never waits. An already-ended ctx always wins, so a cancelled batch can
// not slip into reservation just because the previous turn happened to release.
func (turn ReservationTurn) Wait(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if turn.previous == nil {
		return nil
	}
	select {
	case <-turn.previous:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Release lets the next turn proceed. It is safe to call more than once.
func (turn ReservationTurn) Release() {
	if turn.sequence == nil {
		return
	}
	turn.sequence.once[turn.index].Do(func() { close(turn.sequence.done[turn.index]) })
}

type reservationTurnKey struct{}

// WithReservationTurn attaches a turn to ctx for execution.Submit to honour.
func WithReservationTurn(ctx context.Context, turn ReservationTurn) context.Context {
	return context.WithValue(ctx, reservationTurnKey{}, turn)
}

// ReservationTurnFrom returns the turn attached to ctx, if any. Callers outside
// batched delivery (HTTP submissions, position exits) have none and reserve
// immediately.
func ReservationTurnFrom(ctx context.Context) (ReservationTurn, bool) {
	turn, ok := ctx.Value(reservationTurnKey{}).(ReservationTurn)
	return turn, ok
}
