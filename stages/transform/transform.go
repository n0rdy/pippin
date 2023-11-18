package transform

import (
	"pippin/configs"
	"pippin/functions"
	"pippin/ratelimiter"
	"pippin/stages"
	"pippin/types/statuses"
	"pippin/utils"
	"sync"
	"sync/atomic"
	"time"
)

// Map transforms the input to output using the mapFunc.
// It returns a new stage that can be used to chain other stages.
// The function is executed in the async manner.
//
// This is an intermediate stage function, which means that it can be used only in the middle of the pipeline.
// If you need to set up the pipeline, use functions from the [pipeline] package.
// If you need to get the result of the pipeline, use functions from the [aggregate] and/or [asyncaggregate] packages.
//
// Among other arguments, the function accepts optional stage configs.
// Only the first stage config is used. See [configs.StageConfig] for more details.
//
// If you need to handle errors, use [MapWithError] instead.
func Map[In, Out any](prevStage stages.Stage[In], mapFunc functions.MapFunc[In, Out], confs ...configs.StageConfig) stages.Stage[Out] {
	return transform(prevStage, func(inArg In, outChan chan<- Out, localWg *sync.WaitGroup) {
		defer localWg.Done()
		outChan <- mapFunc(inArg)
	}, confs...)
}

// MapWithError transforms the input to output using the mapFunc.
// Unlike [Map], it also handles errors.
// Unlike [MapWithErrorMapper], it doesn't map errors to the output values, but instead calls the void errorFunc and proceeds to the next input value.
//
// It returns a new stage that can be used to chain other stages.
// The function is executed in the async manner.
//
// This is an intermediate stage function, which means that it can be used only in the middle of the pipeline.
// If you need to set up the pipeline, use functions from the [pipeline] package.
// If you need to get the result of the pipeline, use functions from the [aggregate] and/or [asyncaggregate] packages.
//
// Among other arguments, the function accepts optional stage configs.
// Only the first stage config is used. See [configs.StageConfig] for more details.
//
// If you need to map errors to the output values, use [MapWithErrorMapper] instead.
// If you don't need to handle errors, use [Map] instead.
func MapWithError[In, Out any](prevStage stages.Stage[In], mapFunc functions.MapWithErrFunc[In, Out], errorFunc functions.ErrorFunc, confs ...configs.StageConfig) stages.Stage[Out] {
	return transform(prevStage, func(inArg In, outChan chan<- Out, localWg *sync.WaitGroup) {
		defer localWg.Done()

		out, err := mapFunc(inArg)
		if err != nil {
			errorFunc(err)
			return
		}

		outChan <- out
	}, confs...)
}

// MapWithErrorMapper transforms the input to output using the mapFunc.
// Unlike [Map], it also handles errors.
// Unlike [MapWithError], it maps errors to the output values using the errorMapFunc.
//
// It returns a new stage that can be used to chain other stages.
// The function is executed in the async manner.
//
// This is an intermediate stage function, which means that it can be used only in the middle of the pipeline.
// If you need to set up the pipeline, use functions from the [pipeline] package.
// If you need to get the result of the pipeline, use functions from the [aggregate] and/or [asyncaggregate] packages.
//
// Among other arguments, the function accepts optional stage configs.
// Only the first stage config is used. See [configs.StageConfig] for more details.
//
// If you need to handle errors in a form of a side effect without mapping to the output values, use [MapWithError] instead.
// If you don't need to handle errors, use [Map] instead.
func MapWithErrorMapper[In, Out any](prevStage stages.Stage[In], mapFunc functions.MapWithErrFunc[In, Out], errorMapFunc functions.ErrorMapFunc[Out], confs ...configs.StageConfig) stages.Stage[Out] {
	return transform(prevStage, func(inArg In, outChan chan<- Out, localWg *sync.WaitGroup) {
		defer localWg.Done()

		out, err := mapFunc(inArg)
		if err != nil {
			out = errorMapFunc(err)
		}

		outChan <- out
	}, confs...)
}

// FlatMap transforms the input to output using the flatMapFunc.
// It returns a new stage that can be used to chain other stages.
// The function is executed in the async manner.
//
// This is an intermediate stage function, which means that it can be used only in the middle of the pipeline.
// If you need to set up the pipeline, use functions from the [pipeline] package.
// If you need to get the result of the pipeline, use functions from the [aggregate] and/or [asyncaggregate] packages.
//
// Among other arguments, the function accepts optional stage configs.
// Only the first stage config is used. See [configs.StageConfig] for more details.
//
// If you need to handle errors, use [FlatMapWithError] instead.
func FlatMap[In ~[]E, E, Out any](prevStage stages.Stage[In], flatMapFunc functions.MapFunc[E, Out], confs ...configs.StageConfig) stages.Stage[Out] {
	return transformAsync(prevStage, func(inArg In, outChan chan<- Out, localWg *sync.WaitGroup, pipelineRateLimiter *ratelimiter.RateLimiter, localRateLimiter *ratelimiter.RateLimiter, numOfAsyncWorkers *atomic.Int64) {
		defer localWg.Done()

		funcWg := &sync.WaitGroup{}
		for _, e := range inArg {
			// to make sure that at least 1 goroutine is running regardless of the pipeline rate limiter (if configured)
			acquired := ratelimiter.AcquireSafelyIfRunning(prevStage.PipelineRateLimiter, numOfAsyncWorkers)
			ratelimiter.AcquireSafely(localRateLimiter)
			funcWg.Add(1)

			go func(elem E, pipelineRateLimiterAcquired bool) {
				defer ratelimiter.ReleaseSafelyIfAcquired(prevStage.PipelineRateLimiter, pipelineRateLimiterAcquired, numOfAsyncWorkers)
				defer ratelimiter.ReleaseSafely(localRateLimiter)
				defer funcWg.Done()

				outChan <- flatMapFunc(elem)
			}(e, acquired)
		}
		funcWg.Wait()
	}, confs...)
}

// FlatMapWithError transforms the input to output using the flatMapFunc.
// Unlike [FlatMap], it also handles errors.
// Unlike [FlatMapWithErrorMapper], it doesn't map errors to the output values, but instead calls the void errorFunc and proceeds to the next input value.
//
// It returns a new stage that can be used to chain other stages.
// The function is executed in the async manner.
//
// This is an intermediate stage function, which means that it can be used only in the middle of the pipeline.
// If you need to set up the pipeline, use functions from the [pipeline] package.
// If you need to get the result of the pipeline, use functions from the [aggregate] and/or [asyncaggregate] packages.
//
// Among other arguments, the function accepts optional stage configs.
// Only the first stage config is used. See [configs.StageConfig] for more details.
//
// If you don't need to handle errors, use [FlatMap] instead.
func FlatMapWithError[In ~[]E, E, Out any](prevStage stages.Stage[In], flatMapFunc functions.MapWithErrFunc[E, Out], errorFunc functions.ErrorFunc, confs ...configs.StageConfig) stages.Stage[Out] {
	return transformAsync(prevStage, func(inArg In, outChan chan<- Out, localWg *sync.WaitGroup, pipelineRateLimiter *ratelimiter.RateLimiter, localRateLimiter *ratelimiter.RateLimiter, numOfAsyncWorkers *atomic.Int64) {
		defer localWg.Done()

		funcWg := &sync.WaitGroup{}
		for _, e := range inArg {
			// to make sure that at least 1 goroutine is running regardless of the pipeline rate limiter (if configured)
			acquired := ratelimiter.AcquireSafelyIfRunning(prevStage.PipelineRateLimiter, numOfAsyncWorkers)
			ratelimiter.AcquireSafely(localRateLimiter)
			funcWg.Add(1)

			go func(elem E, pipelineRateLimiterAcquired bool) {
				defer ratelimiter.ReleaseSafelyIfAcquired(prevStage.PipelineRateLimiter, pipelineRateLimiterAcquired, numOfAsyncWorkers)
				defer ratelimiter.ReleaseSafely(localRateLimiter)
				defer funcWg.Done()

				out, err := flatMapFunc(elem)
				if err != nil {
					errorFunc(err)
					return
				}

				outChan <- out
			}(e, acquired)
		}
		funcWg.Wait()
	}, confs...)
}

// FlatMapWithErrorMapper transforms the input to output using the flatMapFunc.
// Unlike [FlatMap], it also handles errors.
// Unlike [FlatMapWithError], it maps errors to the output values using the errorMapFunc.
//
// It returns a new stage that can be used to chain other stages.
// The function is executed in the async manner.
//
// This is an intermediate stage function, which means that it can be used only in the middle of the pipeline.
// If you need to set up the pipeline, use functions from the [pipeline] package.
// If you need to get the result of the pipeline, use functions from the [aggregate] and/or [asyncaggregate] packages.
//
// Among other arguments, the function accepts optional stage configs.
// Only the first stage config is used. See [configs.StageConfig] for more details.
//
// If you need to handle errors in a form of a side effect without mapping to the output values, use [FlatMapWithError] instead.
// If you don't need to handle errors, use [FlatMap] instead.
func FlatMapWithErrorMapper[In ~[]E, E, Out any](prevStage stages.Stage[In], flatMapFunc functions.MapWithErrFunc[E, Out], errorMapFunc functions.ErrorMapFunc[Out], confs ...configs.StageConfig) stages.Stage[Out] {
	return transformAsync(prevStage, func(inArg In, outChan chan<- Out, localWg *sync.WaitGroup, pipelineRateLimiter *ratelimiter.RateLimiter, localRateLimiter *ratelimiter.RateLimiter, numOfAsyncWorkers *atomic.Int64) {
		defer localWg.Done()

		funcWg := &sync.WaitGroup{}
		for _, e := range inArg {
			// to make sure that at least 1 goroutine is running regardless of the pipeline rate limiter (if configured)
			acquired := ratelimiter.AcquireSafelyIfRunning(prevStage.PipelineRateLimiter, numOfAsyncWorkers)
			ratelimiter.AcquireSafely(localRateLimiter)
			funcWg.Add(1)

			go func(elem E, pipelineRateLimiterAcquired bool) {
				defer ratelimiter.ReleaseSafelyIfAcquired(prevStage.PipelineRateLimiter, pipelineRateLimiterAcquired, numOfAsyncWorkers)
				defer ratelimiter.ReleaseSafely(localRateLimiter)
				defer funcWg.Done()

				out, err := flatMapFunc(elem)
				if err != nil {
					out = errorMapFunc(err)
				}

				outChan <- out
			}(e, acquired)
		}
		funcWg.Wait()
	}, confs...)
}

// Filter filters the input using the filterFunc.
// It returns a new stage that can be used to chain other stages.
// The function is executed in the async manner.
//
// This is an intermediate stage function, which means that it can be used only in the middle of the pipeline.
// If you need to set up the pipeline, use functions from the [pipeline] package.
// If you need to get the result of the pipeline, use functions from the [aggregate] and/or [asyncaggregate] packages.
//
// Among other arguments, the function accepts optional stage configs.
// Only the first stage config is used. See [configs.StageConfig] for more details.
func Filter[In any](prevStage stages.Stage[In], filterFunc functions.FilterFunc[In], confs ...configs.StageConfig) stages.Stage[In] {
	return transform(prevStage, func(inArg In, outChan chan<- In, localWg *sync.WaitGroup) {
		defer localWg.Done()
		if filterFunc(inArg) {
			outChan <- inArg
		}
	}, confs...)
}

func transform[In, Out any](prevStage stages.Stage[In], transformFunc func(inArg In, outChan chan<- Out, localWg *sync.WaitGroup), confs ...configs.StageConfig) stages.Stage[Out] {
	inChan := prevStage.Chan
	outChan := make(chan Out)

	var nextStageStarter chan struct{}
	if prevStage.Starter != nil {
		nextStageStarter = make(chan struct{})
	}

	localRateLimiter := localRateLimiter(prevStage.StageRateLimiter, confs...)
	localTimeout := localTimeout(confs...)

	customStageId := customStageId(confs...)
	if customStageId != 0 {
		prevStage.Id = customStageId
	}

	go func() {
		defer ratelimiter.CloseSafely(localRateLimiter)
		defer close(outChan)

		if prevStage.Starter != nil {
			select {
			case _, ok := <-prevStage.Starter:
				if ok {
					nextStageStarter <- struct{}{}
					close(prevStage.Starter)
				}
			case <-prevStage.Context().Done():
				close(prevStage.Starter)
				// if the pipeline is interrupted before it is started, then return
				return
			}
		}

		var timeoutTimer *time.Timer
		go func() {
			if localTimeout > 0 {
				timeoutTimer = time.AfterFunc(localTimeout, func() {
					prevStage.InterruptPipeline()
					prevStage.SetPipelineStatus(statuses.TimedOut)
				})
			}
		}()

		numOfWorkers := &atomic.Int64{}
		localWg := &sync.WaitGroup{}
		running := true
		for running {
			select {
			case in, ok := <-inChan:
				if ok {
					// to make sure that at least 1 goroutine is running regardless of the pipeline rate limiter (if configured)
					acquired := ratelimiter.AcquireSafelyIfRunning(prevStage.PipelineRateLimiter, numOfWorkers)
					ratelimiter.AcquireSafely(localRateLimiter)

					localWg.Add(1)

					go func(inArg In, pipelineRateLimiterAcquired bool) {
						defer ratelimiter.ReleaseSafelyIfAcquired(prevStage.PipelineRateLimiter, pipelineRateLimiterAcquired, numOfWorkers)
						defer ratelimiter.ReleaseSafely(localRateLimiter)

						transformFunc(inArg, outChan, localWg)
					}(in, acquired)
				} else {
					running = false
				}
			case <-prevStage.Context().Done():
				utils.DrainChan(inChan)
				running = false
			}
		}

		localWg.Wait()
		utils.StopSafely(timeoutTimer)
	}()

	return stages.FromStage(prevStage, outChan, nextStageStarter)
}

func transformAsync[In, Out any](prevStage stages.Stage[In], transformAsyncFunc func(inArg In, outChan chan<- Out, localWg *sync.WaitGroup, pipelineRateLimiter *ratelimiter.RateLimiter, localRateLimiter *ratelimiter.RateLimiter, numOfWorkers *atomic.Int64), confs ...configs.StageConfig) stages.Stage[Out] {
	inChan := prevStage.Chan
	outChan := make(chan Out)

	var nextStageStarter chan struct{}
	if prevStage.Starter != nil {
		nextStageStarter = make(chan struct{})
	}

	localRateLimiter := localRateLimiter(prevStage.StageRateLimiter, confs...)
	localTimeout := localTimeout(confs...)

	customStageId := customStageId(confs...)
	if customStageId != 0 {
		prevStage.Id = customStageId
	}

	go func() {
		defer ratelimiter.CloseSafely(localRateLimiter)
		defer close(outChan)

		if prevStage.Starter != nil {
			select {
			case _, ok := <-prevStage.Starter:
				if ok {
					nextStageStarter <- struct{}{}
					close(prevStage.Starter)
				}
			case <-prevStage.Context().Done():
				close(prevStage.Starter)
				// if the pipeline is interrupted before it is started, then return
				return
			}
		}

		var timeoutTimer *time.Timer
		go func() {
			if localTimeout > 0 {
				timeoutTimer = time.AfterFunc(localTimeout, func() {
					prevStage.InterruptPipeline()
					prevStage.SetPipelineStatus(statuses.TimedOut)
				})
			}
		}()

		numOfWorkers := &atomic.Int64{}
		numOfAsyncWorkers := &atomic.Int64{}
		localWg := &sync.WaitGroup{}
		running := true
		for running {
			select {
			case in, ok := <-inChan:
				if ok {
					// to make sure that at least 1 goroutine is running regardless of the pipeline rate limiter (if configured)
					acquired := ratelimiter.AcquireSafelyIfRunning(prevStage.PipelineRateLimiter, numOfWorkers)
					ratelimiter.AcquireSafely(localRateLimiter)

					localWg.Add(1)

					go func(inArg In, pipelineRateLimiterAcquired bool, numOfAsyncWorkersArg *atomic.Int64) {
						defer ratelimiter.ReleaseSafelyIfAcquired(prevStage.PipelineRateLimiter, pipelineRateLimiterAcquired, numOfWorkers)
						defer ratelimiter.ReleaseSafely(localRateLimiter)

						transformAsyncFunc(inArg, outChan, localWg, prevStage.PipelineRateLimiter, localRateLimiter, numOfAsyncWorkersArg)
					}(in, acquired, numOfAsyncWorkers)
				} else {
					running = false
				}
			case <-prevStage.Context().Done():
				utils.DrainChan(inChan)
				running = false
			}
		}

		localWg.Wait()
		utils.StopSafely(timeoutTimer)
	}()

	return stages.FromStage(prevStage, outChan, nextStageStarter)
}

func localRateLimiter(stageRateLimiter *ratelimiter.RateLimiter, confs ...configs.StageConfig) *ratelimiter.RateLimiter {
	if len(confs) == 0 {
		return nil
	}

	conf := confs[0]
	if conf.MaxGoroutines > 0 {
		// stage configs overrides pipeline configs for stage rate limiting
		defer ratelimiter.CloseSafely(stageRateLimiter)
		return ratelimiter.NewRateLimiter(conf.MaxGoroutines)
	}
	return stageRateLimiter
}

func localTimeout(confs ...configs.StageConfig) time.Duration {
	if len(confs) == 0 {
		return 0
	}

	conf := confs[0]
	return time.Duration(conf.TimeoutInMillis) * time.Millisecond
}

func customStageId(confs ...configs.StageConfig) int64 {
	if len(confs) == 0 {
		return 0
	}

	conf := confs[0]
	return conf.CustomId
}
