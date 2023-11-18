package ratelimiter

import "sync/atomic"

// RateLimiter is a simple rate limiter that uses a buffered channel with defined size to limit the number of goroutines.
// Please, note that the real number of goroutines is always greater than the defined size, as:
// - there are service goroutines that are not limited by the rate limiter
// - even if the rate limiter is full, the program will spawn a new goroutine if there is no workers for the current stage
type RateLimiter struct {
	ch  chan struct{}
	max int
}

// AcquireSafely acquires a resource from the rate limiter.
// The function is safe to call with a nil rate limiter.
func AcquireSafely(rl *RateLimiter) {
	if rl != nil {
		rl.Acquire()
	}
}

// AcquireSafelyIfRunning acquires a resource from the rate limiter if the number of workers is greater than zero.
// It is designed specially for the pipeline rate limiter to make sure that none stage is stale, as this leads to a deadlock.
// It means that even if the rate limiter is full, the program will spawn a new goroutine if there is no workers for the current stage.
//
// Please, make sure that numberOfWorkers is not incremented/decremented outside of this function.
//
// Returns true if the resource was acquired, false otherwise.
// This value should be passed to [ReleaseSafelyIfAcquired] in order to release the resource.
func AcquireSafelyIfRunning(rl *RateLimiter, numOfWorkers *atomic.Int64) bool {
	if rl != nil && numOfWorkers.Load() > 0 {
		numOfWorkers.Add(1)
		rl.Acquire()
		return true
	}
	return false
}

// ReleaseSafely releases a resource from the rate limiter.
// The function is safe to call with a nil rate limiter.
func ReleaseSafely(rl *RateLimiter) {
	if rl != nil {
		rl.Release()
	}
}

// ReleaseSafelyIfAcquired releases a resource from the rate limiter if it was acquired.
// Please, make sure that numberOfWorkers is not incremented/decremented outside of this function.
func ReleaseSafelyIfAcquired(rl *RateLimiter, acquired bool, numOfWorkers *atomic.Int64) {
	if acquired {
		numOfWorkers.Add(-1)
		rl.Release()
	}
}

// CloseSafely closes the rate limiter.
// The function is safe to call with a nil rate limiter.
func CloseSafely(rl *RateLimiter) {
	if rl != nil {
		rl.Close()
	}
}

// NewRateLimiter creates a new rate limiter with the given max number of goroutines.
func NewRateLimiter(max int) *RateLimiter {
	return &RateLimiter{
		ch:  make(chan struct{}, max),
		max: max,
	}
}

// Copy creates a copy of the given rate limiter.
// The "copy" means that the new rate limiter will have the same max number of goroutines, but its own channel.
func Copy(rateLimiter *RateLimiter) *RateLimiter {
	if rateLimiter == nil {
		return nil
	}
	return &RateLimiter{
		ch:  make(chan struct{}, rateLimiter.max),
		max: rateLimiter.max,
	}
}

// Acquire acquires a resource from the rate limiter.
// If the rate limiter is full, the function blocks until a resource is released.
// Use [RateLimiter.Release] to release a resource.
func (r *RateLimiter) Acquire() {
	r.ch <- struct{}{}
}

// Release releases a resource from the rate limiter.
// If the rate limiter is empty, the function blocks until a resource is acquired.
// If this situation has happened, there is a bug in the library implementation - please, create an issue in the GitHub repository.
func (r *RateLimiter) Release() {
	<-r.ch
}

// Close closes the rate limiter and releases all resources.
func (r *RateLimiter) Close() error {
	close(r.ch)
	return nil
}
