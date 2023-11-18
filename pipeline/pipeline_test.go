package pipeline

import (
	"pippin/types"
	"pippin/types/statuses"
	"testing"
)

func TestFromSlice_Success(t *testing.T) {
	s := []int{1, 2, 3}

	p := FromSlice(s)

	if p.Status != statuses.Running {
		t.Errorf("Expected status %s, got %s", statuses.Status(statuses.Running).String(), p.Status.String())
	}

	for _, e := range s {
		eFromChan := <-p.InitStage.Chan
		if e != eFromChan {
			t.Errorf("Expected element %d, got %d", e, eFromChan)
		}
	}

	if p.rateLimiter != nil {
		t.Errorf("Expected rateLimiter %v, got %v", nil, p.rateLimiter)
	}
	if p.starter != nil {
		t.Errorf("Expected starter %v, got %v", nil, p.starter)
	}

	p.Interrupt()

	if p.Status != statuses.Interrupted {
		t.Errorf("Expected status %s, got %s", statuses.Status(statuses.Interrupted).String(), p.Status.String())
	}
}

func TestFromMap_Success(t *testing.T) {
	m := map[int]int{1: 1, 2: 2, 3: 3}

	p := FromMap(m)

	if p.Status != statuses.Running {
		t.Errorf("Expected status %s, got %s", statuses.Status(statuses.Running).String(), p.Status.String())
	}

	for k, v := range m {
		eFromChan := <-p.InitStage.Chan
		if eFromChan.First != k || eFromChan.Second != v {
			t.Errorf("Expected element %d, got %d", types.Tuple[int, int]{
				First:  k,
				Second: v,
			}, eFromChan)
		}
	}

	if p.rateLimiter != nil {
		t.Errorf("Expected rateLimiter %v, got %v", nil, p.rateLimiter)
	}
	if p.starter != nil {
		t.Errorf("Expected starter %v, got %v", nil, p.starter)
	}

	p.Interrupt()

	if p.Status != statuses.Interrupted {
		t.Errorf("Expected status %s, got %s", statuses.Status(statuses.Interrupted).String(), p.Status.String())
	}
}

func TestFromChannel_Success(t *testing.T) {
	ch := make(chan int)
	defer close(ch)

	p := FromChannel(ch)

	if p.Status != statuses.Running {
		t.Errorf("Expected status %s, got %s", statuses.Status(statuses.Running).String(), p.Status.String())
	}

	for i := 0; i < 3; i++ {
		ch <- i
		eFromChan := <-p.InitStage.Chan
		if eFromChan != i {
			t.Errorf("Expected element %d, got %d", i, eFromChan)
		}
	}

	if p.rateLimiter != nil {
		t.Errorf("Expected rateLimiter %v, got %v", nil, p.rateLimiter)
	}
	if p.starter != nil {
		t.Errorf("Expected starter %v, got %v", nil, p.starter)
	}

	p.Interrupt()

	if p.Status != statuses.Interrupted {
		t.Errorf("Expected status %s, got %s", statuses.Status(statuses.Interrupted).String(), p.Status.String())
	}
}
