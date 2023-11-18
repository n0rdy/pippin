package utils

import "time"

// StopSafely stops the timer if it is not nil.
func StopSafely(t *time.Timer) {
	if t != nil {
		t.Stop()
	}
}
