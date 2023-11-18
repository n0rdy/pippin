package utils

// DrainChan drains the channel until it is closed.
// It is used to make sure that all the goroutines that have already written to the channel are finished.
// This is required for the case when the pipeline is interrupted, as otherwise the goroutines would be leaked.
func DrainChan[In any](inChan <-chan In) {
	for range inChan {
	}
}
