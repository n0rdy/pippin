package types

import (
	"testing"
	"time"
)

func TestFuture_Success_Get(t *testing.T) {
	future := NewFuture[int]()
	if future.IsDone() {
		t.Error("Future should not be completed")
	}

	future.Complete(42)
	if !future.IsDone() {
		t.Error("Future should be completed")
	}

	result, err := future.Get()
	if err != nil {
		t.Error("Expected no error, got: ", err)
	}
	if *result != 42 {
		t.Error("Expected result 42, got: ", *result)
	}
}

func TestFuture_Success_GetWithTimeout(t *testing.T) {
	future := NewFuture[int]()
	if future.IsDone() {
		t.Error("Future should not be completed")
	}

	future.Complete(42)
	if !future.IsDone() {
		t.Error("Future should be completed")
	}

	result, err := future.GetWithTimeout(time.Duration(1000) * time.Millisecond)
	if err != nil {
		t.Error("Expected no error, got: ", err)
	}
	if *result != 42 {
		t.Error("Expected result 42, got: ", *result)
	}
}

func TestFuture_Failure_GetWithTimeout(t *testing.T) {
	future := NewFuture[int]()
	if future.IsDone() {
		t.Error("Future should not be completed")
	}

	result, err := future.GetWithTimeout(time.Duration(1000) * time.Millisecond)
	if err == nil {
		t.Error("Expected error, got: ", err)
	}
	if result != nil {
		t.Error("Expected no result, got: ", *result)
	}

	future.Complete(42)

	result, err = future.GetWithTimeout(time.Duration(1000) * time.Millisecond)
	if err != nil {
		t.Error("Expected no error, got: ", err)
	}
	if *result != 42 {
		t.Error("Expected result 42, got: ", *result)
	}
}
