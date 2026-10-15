package port

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestReservationSequenceFirstTurnDoesNotWait(t *testing.T) {
	sequence := NewReservationSequence(3)
	if err := sequence.Turn(0).Wait(context.Background()); err != nil {
		t.Fatalf("Turn(0).Wait() error = %v", err)
	}
}

func TestReservationSequenceReleasesInOrder(t *testing.T) {
	const count = 20
	sequence := NewReservationSequence(count)
	var (
		mu    sync.Mutex
		order []int
		wg    sync.WaitGroup
	)
	// Start the turns in reverse so that only the sequence can impose order.
	for index := count - 1; index >= 0; index-- {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			turn := sequence.Turn(index)
			if err := turn.Wait(context.Background()); err != nil {
				t.Errorf("Turn(%d).Wait() error = %v", index, err)
				return
			}
			mu.Lock()
			order = append(order, index)
			mu.Unlock()
			turn.Release()
		}(index)
	}
	wg.Wait()
	for position, index := range order {
		if position != index {
			t.Fatalf("reservation order = %v, want ascending", order)
		}
	}
}

func TestReservationTurnWaitBlocksUntilPreviousReleased(t *testing.T) {
	sequence := NewReservationSequence(2)
	waited := make(chan error, 1)
	go func() { waited <- sequence.Turn(1).Wait(context.Background()) }()
	select {
	case err := <-waited:
		t.Fatalf("Turn(1).Wait() returned %v before Turn(0) was released", err)
	case <-time.After(50 * time.Millisecond):
	}
	sequence.Turn(0).Release()
	select {
	case err := <-waited:
		if err != nil {
			t.Fatalf("Turn(1).Wait() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Turn(1).Wait() did not return after Turn(0) was released")
	}
}

func TestReservationTurnWaitHonoursContext(t *testing.T) {
	sequence := NewReservationSequence(2)
	ctx, cancel := context.WithCancel(context.Background())
	waited := make(chan error, 1)
	go func() { waited <- sequence.Turn(1).Wait(ctx) }()
	cancel()
	select {
	case err := <-waited:
		if err != context.Canceled {
			t.Fatalf("Turn(1).Wait() error = %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Turn(1).Wait() ignored context cancellation")
	}
}

func TestReservationTurnReleaseIsIdempotent(t *testing.T) {
	sequence := NewReservationSequence(2)
	turn := sequence.Turn(0)
	turn.Release()
	turn.Release()
	// A second handle for the same position must also be safe.
	sequence.Turn(0).Release()
	if err := sequence.Turn(1).Wait(context.Background()); err != nil {
		t.Fatalf("Turn(1).Wait() error = %v", err)
	}
}

func TestReservationTurnContextRoundTrip(t *testing.T) {
	if _, ok := ReservationTurnFrom(context.Background()); ok {
		t.Fatal("empty context reported a reservation turn")
	}
	turn := NewReservationSequence(1).Turn(0)
	got, ok := ReservationTurnFrom(WithReservationTurn(context.Background(), turn))
	if !ok {
		t.Fatal("reservation turn was not found in context")
	}
	got.Release()
	if err := NewReservationSequence(1).Turn(0).Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestZeroReservationTurnIsInert(t *testing.T) {
	var turn ReservationTurn
	if err := turn.Wait(context.Background()); err != nil {
		t.Fatalf("zero turn Wait() error = %v", err)
	}
	turn.Release()
}

func TestReservationTurnWaitFailsOnEndedContextEvenWhenPreviousReleased(t *testing.T) {
	sequence := NewReservationSequence(3)
	sequence.Turn(0).Release()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for _, index := range []int{0, 1} {
		for attempt := 0; attempt < 50; attempt++ {
			if err := sequence.Turn(index).Wait(ctx); err != context.Canceled {
				t.Fatalf("Turn(%d).Wait() error = %v, want context.Canceled", index, err)
			}
		}
	}
}
