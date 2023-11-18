package types

import (
	"context"
	"errors"
	"golang.org/x/sync/semaphore"
	"time"
)

// Future is a type that represents a value that will be available in the future.
// It is heavily inspired by Java's Future interface.
// It is used to represent the result of an asynchronous operation and a way to early return from a function.
//
// The creation and completion of the future is done internally, so the user is not expected to create it manually.
// The user is responsible for obtaining the value from the future.
// There are two ways to do that:
// 1. By calling Get() method. This method will block until the value is available. It returns either the pointer to the value or an error.
// In pippin the error means that the pipeline was interrupted before it could complete that's why the value is not available.
// 2. By calling GetWithTimeout(timeout time.Duration) method. This method will block until the value is available or the timeout is reached.
//
// The recommended way to obtain the value is by calling GetWithTimeout() method, as otherwise the execution might be blocked forever.
//
// It is possible to manually check whether the future is done or not by calling IsDone() method.
// This method return a boolean value indicating whether the future is done or not. It is not blocking.
//
// Please, note that since it's the async operation, the value might not be available immediately.
type Future[T any] struct {
	sem   *semaphore.Weighted
	done  bool
	value *T
	err   error
}

// NewFuture creates a new future.
func NewFuture[T any]() Future[T] {
	sem := semaphore.NewWeighted(1)
	err := sem.Acquire(context.Background(), 1)
	if err != nil {
		// this should never happen, as the semaphore is created one line before - please, report an issue if it does
		panic(err)
	}

	return Future[T]{
		sem:  sem,
		done: false,
	}
}

// Get returns the value from the future.
// It blocks until the value is available.
// It returns either the pointer to the value or an error.
// The error means that the pipeline was interrupted before it could complete that's why the value is not available.
//
// Use it with caution as it might block the execution forever.
// As an alternative consider using GetWithTimeout() method.
func (f *Future[T]) Get() (*T, error) {
	if !f.done {
		err := f.sem.Acquire(context.Background(), 1)
		if err != nil {
			// this should never happen - please, report an issue if it does
			return nil, err
		}
	}
	return f.value, f.err
}

// GetWithTimeout returns the value from the future.
// It blocks until the value is available or the timeout is reached.
// It returns either the pointer to the value or an error.
// The error means that the pipeline was interrupted before it could complete that's why the value is not available.
//
// This is the recommended way to obtain the value from the future.
func (f *Future[T]) GetWithTimeout(timeout time.Duration) (*T, error) {
	if !f.done {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()

		err := f.sem.Acquire(ctx, 1)
		if err != nil {
			return nil, errors.New("timeout")
		}
	}
	return f.value, f.err
}

// IsDone returns a boolean value indicating whether the future is done or not.
// It is not blocking.
// Due to the asynchronous nature of the future, it is possible that the value is not available / not up-to-date immediately.
func (f *Future[T]) IsDone() bool {
	return f.done
}

// Complete completes the future with the provided value.
func (f *Future[T]) Complete(value T) {
	f.value = &value
	f.done = true
	f.sem.Release(1)
}

// Fail completes the future with the provided error.
func (f *Future[T]) Fail(err error) {
	f.err = err
	f.done = true
	f.sem.Release(1)
}
