package asyncaggregate

import (
	"cmp"
	"github.com/n0rdy/pippin/configs"
	"github.com/n0rdy/pippin/functions"
	"github.com/n0rdy/pippin/logging"
	"github.com/n0rdy/pippin/ratelimiter"
	"github.com/n0rdy/pippin/stages"
	"github.com/n0rdy/pippin/types"
	"github.com/n0rdy/pippin/types/statuses"
	"github.com/n0rdy/pippin/utils"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

// Sum is an async aggregation function that sums all the elements from the stage channel.
// This function returns a [types.Future] that will be completed with the sum of all the elements from the stage channel.
// Check [types.Future] for more details.
//
// This is a final stage function, which means that it leads to the end of the pipeline.
// If you need to set up the pipeline, use functions from the [pipeline] package.
// If you need to make more transformations, use functions from the [transform] package instead.
//
// If you need to sum complex numbers, use [SumComplexType] instead.
// If you need to sum numbers of a custom type, use [Reduce] instead.
// If you need the sync version of this function, use the aggregate.Sum instead.
func Sum[In types.Number](prevStage stages.Stage[In], confs ...configs.StageConfig) *types.Future[In] {
	return aggregate(
		prevStage,
		func(aggrRes In, in In, localWg *sync.WaitGroup) In {
			defer localWg.Done()
			return aggrRes + in
		},
		func(aggrRes In) In {
			return aggrRes
		},
		confs...,
	)
}

// SumComplexType is an async aggregation function that sums all the elements from the stage channel.
// This function returns a [types.Future] that will be completed with the sum of all the elements from the stage channel.
// Check [types.Future] for more details.
//
// This is a final stage function, which means that it leads to the end of the pipeline.
// If you need to set up the pipeline, use functions from the [pipeline] package.
// If you need to make more transformations, use functions from the [transform] package instead.
//
// If you need to sum simple numbers, use [Sum] instead.
// If you need to sum numbers of a custom type, use [Reduce] instead.
// If you need the sync version of this function, use the aggregate.SumComplexType instead.
func SumComplexType[In types.ComplexNumber](prevStage stages.Stage[In], confs ...configs.StageConfig) *types.Future[In] {
	return aggregate(
		prevStage,
		func(aggrRes In, in In, localWg *sync.WaitGroup) In {
			defer localWg.Done()
			return aggrRes + in
		},
		func(aggrRes In) In {
			return aggrRes
		},
		confs...,
	)
}

// Avg is an async aggregation function that calculates the average of all the elements from the stage channel.
// This function returns a [types.Future] that will be completed with the average of all the elements from the stage channel.
// Check [types.Future] for more details.
//
// This is a final stage function, which means that it leads to the end of the pipeline.
// If you need to set up the pipeline, use functions from the [pipeline] package.
// If you need to make more transformations, use functions from the [transform] package instead.
//
// If you need to calculate the average of complex numbers, use [AvgComplexType] instead.
// If you need the sync version of this function, use the aggregate.Avg instead.
func Avg[In types.Number](prevStage stages.Stage[In], confs ...configs.StageConfig) *types.Future[float64] {
	return aggregate(
		prevStage,
		func(aggrRes types.AggregationWithCounter[In], in In, localWg *sync.WaitGroup) types.AggregationWithCounter[In] {
			defer localWg.Done()
			return types.AggregationWithCounter[In]{
				Aggregation: aggrRes.Aggregation + in,
				Counter:     aggrRes.Counter + 1,
			}
		},
		func(aggrRes types.AggregationWithCounter[In]) float64 {
			return float64(aggrRes.Aggregation) / float64(aggrRes.Counter)
		},
		confs...,
	)
}

// AvgComplexType is an async aggregation function that calculates the average of all the elements from the stage channel.
// This function returns a [types.Future] that will be completed with the average of all the elements from the stage channel.
// Check [types.Future] for more details.
//
// This is a final stage function, which means that it leads to the end of the pipeline.
// If you need to set up the pipeline, use functions from the [pipeline] package.
// If you need to make more transformations, use functions from the [transform] package instead.
//
// If you need to calculate the average of simple numbers, use [Avg] instead.
// If you need the sync version of this function, use the aggregate.AvgComplexType instead.
func AvgComplexType[In types.ComplexNumber](prevStage stages.Stage[In], confs ...configs.StageConfig) *types.Future[complex128] {
	return aggregate(
		prevStage,
		func(aggrRes types.AggregationWithCounter[In], in In, localWg *sync.WaitGroup) types.AggregationWithCounter[In] {
			defer localWg.Done()
			return types.AggregationWithCounter[In]{
				Aggregation: aggrRes.Aggregation + in,
				Counter:     aggrRes.Counter + 1,
			}
		},
		func(aggrRes types.AggregationWithCounter[In]) complex128 {
			return complex128(aggrRes.Aggregation) / complex(float64(aggrRes.Counter), 0)
		},
		confs...,
	)
}

// Max is an async aggregation function that calculates the maximum of all the elements from the stage channel.
// This function returns a [types.Future] that will be completed with the maximum of all the elements from the stage channel.
// Check [types.Future] for more details.
//
// This is a final stage function, which means that it leads to the end of the pipeline.
// If you need to set up the pipeline, use functions from the [pipeline] package.
// If you need to make more transformations, use functions from the [transform] package instead.
//
// It is not possible to use this function with complex numbers, as Go offers no way to compare them.
// If you need the sync version of this function, use the aggregate.Max instead.
func Max[In types.Number](prevStage stages.Stage[In], confs ...configs.StageConfig) *types.Future[In] {
	return aggregate(
		prevStage,
		func(aggrRes In, in In, localWg *sync.WaitGroup) In {
			defer localWg.Done()
			if aggrRes < in {
				return in
			}
			return aggrRes
		},
		func(aggrRes In) In {
			return aggrRes
		},
		confs...,
	)
}

// Min is an async aggregation function that calculates the minimum of all the elements from the stage channel.
// This function returns a [types.Future] that will be completed with the minimum of all the elements from the stage channel.
// Check [types.Future] for more details.
//
// This is a final stage function, which means that it leads to the end of the pipeline.
// If you need to set up the pipeline, use functions from the [pipeline] package.
// If you need to make more transformations, use functions from the [transform] package instead.
//
// It is not possible to use this function with complex numbers, as Go offers no way to compare them.
// If you need the sync version of this function, use the aggregate.Min instead.
func Min[In types.Number](prevStage stages.Stage[In], confs ...configs.StageConfig) *types.Future[In] {
	return aggregate(
		prevStage,
		func(aggrRes In, in In, localWg *sync.WaitGroup) In {
			defer localWg.Done()
			if aggrRes > in {
				return in
			}
			return aggrRes
		},
		func(aggrRes In) In {
			return aggrRes
		},
		confs...,
	)
}

// Count is an async aggregation function that counts all the elements from the stage channel.
// This function returns a [types.Future] that will be completed with the number of all the elements from the stage channel.
// Check [types.Future] for more details.
//
// This is a final stage function, which means that it leads to the end of the pipeline.
// If you need to set up the pipeline, use functions from the [pipeline] package.
// If you need to make more transformations, use functions from the [transform] package instead.
//
// If you need the sync version of this function, use the aggregate.Count instead.
func Count[In any](prevStage stages.Stage[In], confs ...configs.StageConfig) *types.Future[int64] {
	return aggregate(
		prevStage,
		func(aggrRes int64, in In, localWg *sync.WaitGroup) int64 {
			defer localWg.Done()
			return aggrRes + 1
		},
		func(aggrRes int64) int64 {
			return aggrRes
		},
		confs...,
	)
}

// Sort is an async aggregation function that sorts all the elements from the stage channel in the ascending order.
// This function returns a [types.Future] that will be completed with the sorted slice of all the elements from the stage channel.
// Check [types.Future] for more details.
//
// This is a final stage function, which means that it leads to the end of the pipeline.
// If you need to set up the pipeline, use functions from the [pipeline] package.
// If you need to make more transformations, use functions from the [transform] package instead.
//
// If you need to sort the elements in the descending order, use [SortDesc] instead.
// If you need the sync version of this function, use the aggregate.Sort instead.
func Sort[In cmp.Ordered](prevStage stages.Stage[In], confs ...configs.StageConfig) *types.Future[[]In] {
	return aggregate(
		prevStage,
		func(aggrRes []In, in In, localWg *sync.WaitGroup) []In {
			defer localWg.Done()
			return append(aggrRes, in)
		},
		func(aggrRes []In) []In {
			sort.Slice(aggrRes, func(i, j int) bool {
				return cmp.Less(aggrRes[i], aggrRes[j])
			})
			return aggrRes
		},
		confs...,
	)
}

// SortDesc is an async aggregation function that sorts all the elements from the stage channel in the descending order.
// This function returns a [types.Future] that will be completed with the sorted slice of all the elements from the stage channel.
// Check [types.Future] for more details.
//
// This is a final stage function, which means that it leads to the end of the pipeline.
// If you need to set up the pipeline, use functions from the [pipeline] package.
// If you need to make more transformations, use functions from the [transform] package instead.
//
// If you need to sort the elements in the ascending order, use [Sort] instead.
// If you need the sync version of this function, use the aggregate.SortDesc instead.
func SortDesc[In cmp.Ordered](prevStage stages.Stage[In], confs ...configs.StageConfig) *types.Future[[]In] {
	return aggregate(
		prevStage,
		func(aggrRes []In, in In, localWg *sync.WaitGroup) []In {
			defer localWg.Done()
			return append(aggrRes, in)
		},
		func(aggrRes []In) []In {
			sort.Slice(aggrRes, func(i, j int) bool {
				return cmp.Less(aggrRes[j], aggrRes[i])
			})
			return aggrRes
		},
		confs...,
	)
}

// GroupBy is an async aggregation function that groups all the elements from the stage channel by the given function.
// This function returns a [types.Future] that will be completed with the map of all the elements from the stage channel grouped by the given function.
// Check [types.Future] for more details.
//
// The key of the map is the result of the given function, and the value is the slice of all the elements from the stage channel that have the same result of the given function.
//
// This is a final stage function, which means that it leads to the end of the pipeline.
// If you need to set up the pipeline, use functions from the [pipeline] package.
// If you need to make more transformations, use functions from the [transform] package instead.
//
// If you need the sync version of this function, use the aggregate.GroupBy instead.
func GroupBy[In any, K comparable](prevStage stages.Stage[In], groupByFunc functions.MapFunc[In, K], confs ...configs.StageConfig) *types.Future[map[K][]In] {
	once := &sync.Once{}

	return aggregate(
		prevStage,
		func(aggrRes map[K][]In, in In, localWg *sync.WaitGroup) map[K][]In {
			defer localWg.Done()

			once.Do(func() {
				aggrRes = make(map[K][]In)
			})

			out := groupByFunc(in)
			aggrRes[out] = append(aggrRes[out], in)
			return aggrRes
		},
		func(aggrRes map[K][]In) map[K][]In {
			return aggrRes
		},
		confs...,
	)
}

// Reduce is an async aggregation function that reduces all the elements from the stage channel by the given function.
// This function returns a [types.Future] that will be completed with the result of reducing all the elements from the stage channel by the given function.
// Check [types.Future] for more details.
//
// This is a final stage function, which means that it leads to the end of the pipeline.
// If you need to set up the pipeline, use functions from the [pipeline] package.
// If you need to make more transformations, use functions from the [transform] package instead.
//
// If you need to reduce the elements to a slice, use [AsSlice] instead.
// If you need to reduce the elements to a sorted slice, use [Sort] or [SortDesc] instead.
// If you need to reduce the elements to a map, use [AsMap] instead.
// If you need to reduce the elements to a multimap, use [AsMultiMap] instead.
// If you need to reduce the elements to a single value, use [Sum], [Avg], [Max], [Min] or [Count] instead.
// If you need the sync version of this function, use the aggregate.Reduce instead.
func Reduce[In any](prevStage stages.Stage[In], reduceFunc functions.ReduceFunc[In], confs ...configs.StageConfig) *types.Future[In] {
	return aggregate(
		prevStage,
		func(aggrRes In, in In, localWg *sync.WaitGroup) In {
			defer localWg.Done()
			return reduceFunc(aggrRes, in)
		},
		func(aggrRes In) In {
			return aggrRes
		},
		confs...,
	)
}

// AsSlice is an async aggregation function that reduces all the elements from the stage channel to a slice.
// This function returns a [types.Future] that will be completed with the slice of all the elements from the stage channel.
// Check [types.Future] for more details.
//
// This is a final stage function, which means that it leads to the end of the pipeline.
// If you need to set up the pipeline, use functions from the [pipeline] package.
// If you need to make more transformations, use functions from the [transform] package instead.
//
// If you need to reduce the elements to a sorted slice, use [Sort] or [SortDesc] instead.
// If you need to reduce the elements to a map, use [AsMap] instead.
// If you need to reduce the elements to a multimap, use [AsMultiMap] instead.
// If you need to reduce the elements to a single value, use [Sum], [Avg], [Max], [Min], [Count] or [Reduce] instead.
// If you need the sync version of this function, use the aggregate.AsSlice instead.
func AsSlice[In any](prevStage stages.Stage[In], confs ...configs.StageConfig) *types.Future[[]In] {
	return aggregate(
		prevStage,
		func(aggrRes []In, in In, localWg *sync.WaitGroup) []In {
			defer localWg.Done()
			return append(aggrRes, in)
		},
		func(aggrRes []In) []In {
			return aggrRes
		},
		confs...,
	)
}

// AsMap is an async aggregation function that reduces all the elements from the stage channel to a map.
// This function returns a [types.Future] that will be completed with the map of all the elements from the stage channel.
// Check [types.Future] for more details.
//
// The key of the map is the first element of the tuple returned by the given function, and the value is the second element of the tuple returned by the given function.
//
// This is a final stage function, which means that it leads to the end of the pipeline.
// If you need to set up the pipeline, use functions from the [pipeline] package.
// If you need to make more transformations, use functions from the [transform] package instead.
//
// If you need to reduce the elements to a slice, use [AsSlice] instead.
// If you need to reduce the elements to a sorted slice, use [Sort] or [SortDesc] instead.
// If you need to reduce the elements to a multimap, use [AsMultiMap] instead.
// If you need to reduce the elements to a single value, use [Sum], [Avg], [Max], [Min], [Count] or [Reduce] instead.
// If you need the sync version of this function, use the aggregate.AsMap instead.
func AsMap[In any, K comparable, V any](prevStage stages.Stage[In], mapFunc functions.MapFunc[In, types.Tuple[K, V]], confs ...configs.StageConfig) *types.Future[map[K]V] {
	once := &sync.Once{}

	return aggregate(
		prevStage,
		func(aggrRes map[K]V, in In, localWg *sync.WaitGroup) map[K]V {
			defer localWg.Done()

			once.Do(func() {
				aggrRes = make(map[K]V)
			})

			out := mapFunc(in)
			aggrRes[out.First] = out.Second
			return aggrRes
		},
		func(aggrRes map[K]V) map[K]V {
			return aggrRes
		},
		confs...,
	)
}

// AsMultiMap is an async aggregation function that reduces all the elements from the stage channel to a multimap.
// This function returns a [types.Future] that will be completed with the multimap of all the elements from the stage channel.
// Check [types.Future] for more details.
//
// The key of the map is the first element of the tuple returned by the given function, and the value is the slice of all the second elements of the tuples returned by the given function that have the same first element.
//
// This is a final stage function, which means that it leads to the end of the pipeline.
// If you need to set up the pipeline, use functions from the [pipeline] package.
// If you need to make more transformations, use functions from the [transform] package instead.
//
// If you need to reduce the elements to a slice, use [AsSlice] instead.
// If you need to reduce the elements to a sorted slice, use [Sort] or [SortDesc] instead.
// If you need to reduce the elements to a map, use [AsMap] instead.
// If you need to reduce the elements to a single value, use [Sum], [Avg], [Max], [Min], [Count] or [Reduce] instead.
// If you need the sync version of this function, use the aggregate.AsMultiMap instead.
func AsMultiMap[In any, K comparable, V any](prevStage stages.Stage[In], mapFunc functions.MapFunc[In, types.Tuple[K, V]], confs ...configs.StageConfig) *types.Future[map[K][]V] {
	once := &sync.Once{}

	return aggregate(
		prevStage,
		func(aggrRes map[K][]V, in In, localWg *sync.WaitGroup) map[K][]V {
			defer localWg.Done()

			once.Do(func() {
				aggrRes = make(map[K][]V)
			})

			out := mapFunc(in)
			aggrRes[out.First] = append(aggrRes[out.First], out.Second)
			return aggrRes
		},
		func(aggrRes map[K][]V) map[K][]V {
			return aggrRes
		},
		confs...,
	)
}

// ForEach is an async aggregation function that iterates over all the elements from the stage channel.
// This function returns a [types.Future] that will be completed with the void value.
// Check [types.Future] for more details.
//
// This function is useful when you need to iterate over all the elements from the stage channel, but you don't need to return any value.
//
// This is a final stage function, which means that it leads to the end of the pipeline.
// If you need to set up the pipeline, use functions from the [pipeline] package.
// If you need to make more transformations, use functions from the [transform] package instead.
//
// If you need to return a value, use [Reduce] instead.
// If you need the sync version of this function, use the aggregate.ForEach instead.
func ForEach[In any](prevStage stages.Stage[In], forEachFunc functions.ForEachFunc[In], confs ...configs.StageConfig) *types.Future[types.Void] {
	return aggregate(
		prevStage,
		func(aggrRes types.Void, in In, localWg *sync.WaitGroup) types.Void {
			defer localWg.Done()

			forEachFunc(in)
			return aggrRes
		},
		func(aggrRes types.Void) types.Void {
			return aggrRes
		},
		confs...,
	)
}

// Distinct is an async aggregation function that removes all the duplicate elements from the stage channel.
// The result is in a random order.
// This function returns a [types.Future] that will be completed with the slice of all the distinct elements from the stage channel.
// Check [types.Future] for more details.
//
// This function is useful when you need to remove all the duplicate elements from the stage channel.
//
// Only the comparable types are supported.
//
// This is a final stage function, which means that it leads to the end of the pipeline.
// If you need to set up the pipeline, use functions from the [pipeline] package.
// If you need to make more transformations, use functions from the [transform] package instead.
//
// If you need the sync version of this function, use the aggregate.Distinct instead.
func Distinct[In comparable](prevStage stages.Stage[In], confs ...configs.StageConfig) *types.Future[[]In] {
	once := &sync.Once{}

	return aggregate(
		prevStage,
		func(aggrRes types.AggregationWithCache[In], in In, localWg *sync.WaitGroup) types.AggregationWithCache[In] {
			defer localWg.Done()

			once.Do(func() {
				aggrRes.Cache = make(map[In]bool)
			})

			if _, seen := aggrRes.Cache[in]; !seen {
				aggrRes.Cache[in] = true
				aggrRes.Aggregation = append(aggrRes.Aggregation, in)
			}
			return aggrRes
		},
		func(aggrRes types.AggregationWithCache[In]) []In {
			return aggrRes.Aggregation
		},
		confs...,
	)
}

// DistinctCount is an async aggregation function that counts all the distinct elements from the stage channel.
// This function returns a [types.Future] that will be completed with the number of all the distinct elements from the stage channel.
// Check [types.Future] for more details.
//
// This function is useful when you need to count all the distinct elements from the stage channel.
//
// Only the comparable types are supported.
//
// This is a final stage function, which means that it leads to the end of the pipeline.
// If you need to set up the pipeline, use functions from the [pipeline] package.
// If you need to make more transformations, use functions from the [transform] package instead.
//
// If you need to get the distinct elements, use [Distinct] instead.
// If you need the sync version of this function, use the aggregate.DistinctCount instead.
func DistinctCount[In comparable](prevStage stages.Stage[In], confs ...configs.StageConfig) *types.Future[int64] {
	once := &sync.Once{}

	return aggregate(
		prevStage,
		func(aggrRes types.AggregationWithCacheAndCounter[In], in In, localWg *sync.WaitGroup) types.AggregationWithCacheAndCounter[In] {
			defer localWg.Done()

			once.Do(func() {
				aggrRes.Cache = make(map[In]bool)
			})

			if _, seen := aggrRes.Cache[in]; !seen {
				aggrRes.Cache[in] = true
				aggrRes.Aggregation = append(aggrRes.Aggregation, in)
				aggrRes.Counter++
			}
			return aggrRes
		},
		func(aggrRes types.AggregationWithCacheAndCounter[In]) int64 {
			return aggrRes.Counter
		},
		confs...,
	)
}

// aggregate is a generic async aggregation function that aggregates all the elements from the stage channel by the given functions.
// This function returns a [types.Future] that will be completed with the result of the aggregation.
func aggregate[In, Aggr, Res any](prevStage stages.Stage[In], aggFunc func(aggrRes Aggr, in In, localWg *sync.WaitGroup) Aggr, resFunc func(aggrRes Aggr) Res, confs ...configs.StageConfig) *types.Future[Res] {
	inChan := prevStage.Chan
	var aggrRes Aggr
	m := sync.Mutex{}
	doneChan := make(chan struct{})
	errChan := make(chan error)

	localRateLimiter := localRateLimiter(prevStage.StageRateLimiter, confs...)
	localLogger, stageSpecific := localLogger(prevStage.Logger, confs...)
	if stageSpecific {
		defer localLogger.Close()
	}
	localTimeout := localTimeout(confs...)

	var stageIdAsString string
	customStageId := customStageId(confs...)
	if customStageId != 0 {
		stageIdAsString = "stage " + strconv.FormatInt(customStageId, 10) + ": "
	} else {
		stageIdAsString = "stage " + strconv.FormatInt(prevStage.Id+1, 10) + ": "
	}

	localLogger.Debug(stageIdAsString + "initiating...")

	go func() {
		defer ratelimiter.CloseSafely(localRateLimiter)
		if prevStage.Starter != nil {
			localLogger.Debug(stageIdAsString + "waiting for the start signal...")

			select {
			case _, ok := <-prevStage.Starter:
				if ok {
					localLogger.Debug(stageIdAsString + "start signal received")
					close(prevStage.Starter)
				}
			case <-prevStage.Context().Done():
				localLogger.Debug(stageIdAsString + "context done signal received before the start signal")

				errChan <- prevStage.Context().Err()
				close(prevStage.Starter)
				// if the pipeline is interrupted before it is started, then return
				return
			}
		}

		localLogger.Info(stageIdAsString + "started")

		var timeoutTimer *time.Timer
		go func() {
			if localTimeout > 0 {
				timeoutTimer = time.AfterFunc(localTimeout, func() {
					localLogger.Info(stageIdAsString + "timeout reached for stage - interrupting the pipeline")
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
					localLogger.Debug(stageIdAsString + "input received")
					// to make sure that at least 1 goroutine is running regardless of the pipeline rate limiter (if configured)
					acquired := ratelimiter.AcquireSafelyIfRunning(prevStage.PipelineRateLimiter, numOfWorkers)
					ratelimiter.AcquireSafely(localRateLimiter)

					localWg.Add(1)

					go func(inArg In, pipelineRateLimiterAcquired bool) {
						defer ratelimiter.ReleaseSafelyIfAcquired(prevStage.PipelineRateLimiter, pipelineRateLimiterAcquired, numOfWorkers)
						defer ratelimiter.ReleaseSafely(localRateLimiter)

						m.Lock()
						defer m.Unlock()
						aggrRes = aggFunc(aggrRes, inArg, localWg)
						localLogger.Debug(stageIdAsString + "input processed")
					}(in, acquired)
				} else {
					localLogger.Debug(stageIdAsString + "input channel closed")
					// without this wait, the future might be completed before all the goroutines are finished
					localWg.Wait()
					doneChan <- struct{}{}
					running = false
				}
			case <-prevStage.Context().Done():
				localLogger.Debug(stageIdAsString + "context done signal received")
				utils.DrainChan(inChan)
				errChan <- prevStage.Context().Err()
				running = false
			}
		}

		utils.StopSafely(timeoutTimer)
	}()

	futureRes := types.NewFuture[Res]()

	go func() {
		defer close(doneChan)
		defer close(errChan)

		select {
		case <-doneChan:
			localLogger.Debug(stageIdAsString + "future completed successfully")
			futureRes.Complete(resFunc(aggrRes))
			prevStage.SetPipelineStatus(statuses.Done)
		case err := <-errChan:
			localLogger.Error(stageIdAsString + "future completed with error")
			futureRes.Fail(err)
		}
		localLogger.Info(stageIdAsString + "finished")
	}()

	return &futureRes
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

func localLogger(stageLogger logging.Logger, confs ...configs.StageConfig) (logging.Logger, bool) {
	if len(confs) == 0 {
		return stageLogger, false
	}

	conf := confs[0]
	if conf.Logger != nil {
		// stage configs overrides pipeline configs for logger
		return conf.Logger, true
	}
	return stageLogger, false
}

func localTimeout(confs ...configs.StageConfig) time.Duration {
	if len(confs) == 0 {
		return 0
	}

	conf := confs[0]
	return conf.Timeout
}

func customStageId(confs ...configs.StageConfig) int64 {
	if len(confs) == 0 {
		return 0
	}

	conf := confs[0]
	return conf.CustomId
}
