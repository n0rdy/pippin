package aggregate

import (
	"cmp"
	"github.com/n0rdy/pippin/configs"
	"github.com/n0rdy/pippin/functions"
	"github.com/n0rdy/pippin/logging"
	"github.com/n0rdy/pippin/stages"
	"github.com/n0rdy/pippin/types"
	"github.com/n0rdy/pippin/types/statuses"
	"github.com/n0rdy/pippin/utils"
	"sort"
	"strconv"
	"sync"
	"time"
)

// Sum is a sync aggregation function that sums all the elements from the stage channel.
// This function returns error if the pipeline is interrupted before the stage finishes.
// The error is obtained internally by calling the `context.Err()` function.
//
// This is a final stage function, which means that it leads to the end of the pipeline.
// If you need to set up the pipeline, use functions from the [pipeline] package.
// If you need to make more transformations, use functions from the [transform] package instead.
//
// If you need to sum complex numbers, use [SumComplexType] instead.
// If you need to sum numbers of a custom type, use [Reduce] instead.
// If you need the async version of this function, use the asyncaggregate.Sum instead.
func Sum[In types.Number](prevStage stages.Stage[In], confs ...configs.StageConfig) (*In, error) {
	return aggregate(
		prevStage,
		func(aggrRes In, in In) In {
			return aggrRes + in
		},
		func(aggrRes In) In {
			return aggrRes
		},
		confs...,
	)
}

// SumComplexType is a sync aggregation function that sums all the elements from the stage channel.
// This function returns error if the pipeline is interrupted before the stage finishes.
// The error is obtained internally by calling the `context.Err()` function.
//
// This is a final stage function, which means that it leads to the end of the pipeline.
// If you need to set up the pipeline, use functions from the [pipeline] package.
// If you need to make more transformations, use functions from the [transform] package instead.
//
// If you need to sum simple numbers, use [Sum] instead.
// If you need to sum numbers of a custom type, use [Reduce] instead.
// If you need the async version of this function, use the asyncaggregate.SumComplexType instead.
func SumComplexType[In types.ComplexNumber](prevStage stages.Stage[In], confs ...configs.StageConfig) (*In, error) {
	return aggregate(
		prevStage,
		func(aggrRes In, in In) In {
			return aggrRes + in
		},
		func(aggrRes In) In {
			return aggrRes
		},
		confs...,
	)
}

// Avg is a sync aggregation function that calculates the average of all the elements from the stage channel.
// This function returns error if the pipeline is interrupted before the stage finishes.
// The error is obtained internally by calling the `context.Err()` function.
//
// If you need to calculate the average of complex numbers, use [AvgComplexType] instead.
// If you need the async version of this function, use the asyncaggregate.Avg instead.
func Avg[In types.Number](prevStage stages.Stage[In], confs ...configs.StageConfig) (*float64, error) {
	return aggregate(
		prevStage,
		func(aggrRes types.AggregationWithCounter[In], in In) types.AggregationWithCounter[In] {
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

// AvgComplexType is a sync aggregation function that calculates the average of all the elements from the stage channel.
// This function returns error if the pipeline is interrupted before the stage finishes.
// The error is obtained internally by calling the `context.Err()` function.
//
// This is a final stage function, which means that it leads to the end of the pipeline.
// If you need to set up the pipeline, use functions from the [pipeline] package.
// If you need to make more transformations, use functions from the [transform] package instead.
//
// If you need to calculate the average of simple numbers, use [Avg] instead.
// If you need the async version of this function, use the asyncaggregate.AvgComplexType instead.
func AvgComplexType[In types.ComplexNumber](prevStage stages.Stage[In], confs ...configs.StageConfig) (*complex128, error) {
	return aggregate(
		prevStage,
		func(aggrRes types.AggregationWithCounter[In], in In) types.AggregationWithCounter[In] {
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

// Max is a sync aggregation function that returns the maximum of all the elements from the stage channel.
// This function returns error if the pipeline is interrupted before the stage finishes.
// The error is obtained internally by calling the `context.Err()` function.
//
// This is a final stage function, which means that it leads to the end of the pipeline.
// If you need to set up the pipeline, use functions from the [pipeline] package.
// If you need to make more transformations, use functions from the [transform] package instead.
//
// It is not possible to use this function with complex numbers, as Go offers no way to compare them.
// If you need the async version of this function, use the asyncaggregate.Max instead.
func Max[In types.Number](prevStage stages.Stage[In], confs ...configs.StageConfig) (*In, error) {
	return aggregate(
		prevStage,
		func(aggrRes In, in In) In {
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

// Min is a sync aggregation function that returns the minimum of all the elements from the stage channel.
// This function returns error if the pipeline is interrupted before the stage finishes.
// The error is obtained internally by calling the `context.Err()` function.
//
// This is a final stage function, which means that it leads to the end of the pipeline.
// If you need to set up the pipeline, use functions from the [pipeline] package.
// If you need to make more transformations, use functions from the [transform] package instead.
//
// It is not possible to use this function with complex numbers, as Go offers no way to compare them.
// If you need the async version of this function, use the asyncaggregate.Min instead.
func Min[In types.Number](prevStage stages.Stage[In], confs ...configs.StageConfig) (*In, error) {
	return aggregate(
		prevStage,
		func(aggrRes In, in In) In {
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

// Count is a sync aggregation function that counts all the elements from the stage channel.
// This function returns error if the pipeline is interrupted before the stage finishes.
// The error is obtained internally by calling the `context.Err()` function.
//
// This is a final stage function, which means that it leads to the end of the pipeline.
// If you need to set up the pipeline, use functions from the [pipeline] package.
// If you need to make more transformations, use functions from the [transform] package instead.
//
// If you need the async version of this function, use the asyncaggregate.Count instead.
func Count[In any](prevStage stages.Stage[In], confs ...configs.StageConfig) (*int64, error) {
	return aggregate(
		prevStage,
		func(aggrRes int64, in In) int64 {
			return aggrRes + 1
		},
		func(aggrRes int64) int64 {
			return aggrRes
		},
		confs...,
	)
}

// Sort is a sync aggregation function that sorts all the elements from the stage channel in the ascending order and returns them as a slice.
// This function returns error if the pipeline is interrupted before the stage finishes.
// The error is obtained internally by calling the `context.Err()` function.
//
// Only the ordered types are supported.
//
// This is a final stage function, which means that it leads to the end of the pipeline.
// If you need to set up the pipeline, use functions from the [pipeline] package.
// If you need to make more transformations, use functions from the [transform] package instead.
//
// It is not possible to use this function with complex numbers, as Go offers no way to compare them.
// If you need to sort numbers in the descending order, use [SortDesc] instead.
// If you need the async version of this function, use the asyncaggregate.Sort instead.
func Sort[In cmp.Ordered](prevStage stages.Stage[In], confs ...configs.StageConfig) (*[]In, error) {
	return aggregate(
		prevStage,
		func(aggrRes []In, in In) []In {
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

// SortDesc is a sync aggregation function that sorts all the elements from the stage channel in the descending order and returns them as a slice.
// This function returns error if the pipeline is interrupted before the stage finishes.
// The error is obtained internally by calling the `context.Err()` function.
//
// Only the ordered types are supported.
//
// This is a final stage function, which means that it leads to the end of the pipeline.
// If you need to set up the pipeline, use functions from the [pipeline] package.
// If you need to make more transformations, use functions from the [transform] package instead.
//
// It is not possible to use this function with complex numbers, as Go offers no way to compare them.
// If you need to sort numbers in the ascending order, use [Sort] instead.
// If you need the async version of this function, use the asyncaggregate.SortDesc instead.
func SortDesc[In cmp.Ordered](prevStage stages.Stage[In], confs ...configs.StageConfig) (*[]In, error) {
	return aggregate(
		prevStage,
		func(aggrRes []In, in In) []In {
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

// GroupBy is a sync aggregation function that groups all the elements from the stage channel by the given function and returns them as a map of a key to a slice of values.
// This function returns error if the pipeline is interrupted before the stage finishes.
// The error is obtained internally by calling the `context.Err()` function.
//
// Only the comparable types are supported.
//
// This is a final stage function, which means that it leads to the end of the pipeline.
// If you need to set up the pipeline, use functions from the [pipeline] package.
// If you need to make more transformations, use functions from the [transform] package instead.
//
// If you need the async version of this function, use the asyncaggregate.GroupBy instead.
func GroupBy[In any, K comparable](prevStage stages.Stage[In], groupByFunc functions.MapFunc[In, K], confs ...configs.StageConfig) (*map[K][]In, error) {
	once := &sync.Once{}

	return aggregate(
		prevStage,
		func(aggrRes map[K][]In, in In) map[K][]In {
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

// Reduce is a sync aggregation function that reduces all the elements from the stage channel to a single value using the given function.
// This function returns error if the pipeline is interrupted before the stage finishes.
// The error is obtained internally by calling the `context.Err()` function.
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
// If you need the async version of this function, use the asyncaggregate.Reduce instead.
func Reduce[In any](prevStage stages.Stage[In], reduceFunc functions.ReduceFunc[In], confs ...configs.StageConfig) (*In, error) {
	return aggregate(
		prevStage,
		func(aggrRes In, in In) In {
			return reduceFunc(aggrRes, in)
		},
		func(aggrRes In) In {
			return aggrRes
		},
		confs...,
	)
}

// AsSlice is a sync aggregation function that reduces all the elements from the stage channel to a slice.
// This function returns error if the pipeline is interrupted before the stage finishes.
// The error is obtained internally by calling the `context.Err()` function.
//
// This is a final stage function, which means that it leads to the end of the pipeline.
// If you need to set up the pipeline, use functions from the [pipeline] package.
// If you need to make more transformations, use functions from the [transform] package instead.
//
// If you need to reduce the elements to a sorted slice, use [Sort] or [SortDesc] instead.
// If you need to reduce the elements to a map, use [AsMap] instead.
// If you need to reduce the elements to a multimap, use [AsMultiMap] instead.
// If you need to reduce the elements to a single value, use [Sum], [Avg], [Max], [Min], [Count] or [Reduce] instead.
// If you need the async version of this function, use the asyncaggregate.AsSlice instead.
func AsSlice[In any](prevStage stages.Stage[In], confs ...configs.StageConfig) (*[]In, error) {
	return aggregate(
		prevStage,
		func(aggrRes []In, in In) []In {
			return append(aggrRes, in)
		},
		func(aggrRes []In) []In {
			return aggrRes
		},
		confs...,
	)
}

// AsMap is a sync aggregation function that reduces all the elements from the stage channel to a map.
// This function returns error if the pipeline is interrupted before the stage finishes.
// The error is obtained internally by calling the `context.Err()` function.
//
// Only the comparable types are supported as a map key.
//
// This is a final stage function, which means that it leads to the end of the pipeline.
// If you need to set up the pipeline, use functions from the [pipeline] package.
// If you need to make more transformations, use functions from the [transform] package instead.
//
// If you need to reduce the elements to a slice, use [AsSlice] instead.
// If you need to reduce the elements to a sorted slice, use [Sort] or [SortDesc] instead.
// If you need to reduce the elements to a multimap, use [AsMultiMap] instead.
// If you need to reduce the elements to a single value, use [Sum], [Avg], [Max], [Min], [Count] or [Reduce] instead.
// If you need the async version of this function, use the asyncaggregate.AsMap instead.
func AsMap[In any, K comparable, V any](prevStage stages.Stage[In], mapFunc functions.MapFunc[In, types.Tuple[K, V]], confs ...configs.StageConfig) (*map[K]V, error) {
	once := &sync.Once{}

	return aggregate(
		prevStage,
		func(aggrRes map[K]V, in In) map[K]V {
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

// AsMultiMap is a sync aggregation function that reduces all the elements from the stage channel to a multimap.
// This function returns error if the pipeline is interrupted before the stage finishes.
// The error is obtained internally by calling the `context.Err()` function.
//
// Only the comparable types are supported as a map key.
//
// This is a final stage function, which means that it leads to the end of the pipeline.
// If you need to set up the pipeline, use functions from the [pipeline] package.
// If you need to make more transformations, use functions from the [transform] package instead.
//
// If you need to reduce the elements to a slice, use [AsSlice] instead.
// If you need to reduce the elements to a sorted slice, use [Sort] or [SortDesc] instead.
// If you need to reduce the elements to a map, use [AsMap] instead.
// If you need to reduce the elements to a single value, use [Sum], [Avg], [Max], [Min], [Count] or [Reduce] instead.
// If you need the async version of this function, use the asyncaggregate.AsMultiMap instead.
func AsMultiMap[In any, K comparable, V any](prevStage stages.Stage[In], mapFunc functions.MapFunc[In, types.Tuple[K, V]], confs ...configs.StageConfig) (*map[K][]V, error) {
	once := &sync.Once{}

	return aggregate(
		prevStage,
		func(aggrRes map[K][]V, in In) map[K][]V {
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

// ForEach is a sync aggregation function that iterates over all the elements from the stage channel and calls the given function for each of them.
// This function returns error if the pipeline is interrupted before the stage finishes.
// The error is obtained internally by calling the `context.Err()` function.
//
// This function is useful when you need to iterate over all the elements from the stage channel, but you don't need to return any value.
//
// This is a final stage function, which means that it leads to the end of the pipeline.
// If you need to set up the pipeline, use functions from the [pipeline] package.
// If you need to make more transformations, use functions from the [transform] package instead.
//
// If you need the async version of this function, use the asyncaggregate.ForEach instead.
func ForEach[In any](prevStage stages.Stage[In], forEachFunc functions.ForEachFunc[In], confs ...configs.StageConfig) error {
	_, err := aggregate(
		prevStage,
		func(aggrRes types.Void, in In) types.Void {
			forEachFunc(in)
			return aggrRes
		},
		func(aggrRes types.Void) types.Void {
			return aggrRes
		},
		confs...,
	)

	if err != nil {
		return err
	}
	return nil
}

// Distinct is a sync aggregation function that returns all the distinct elements from the stage channel as a slice.
// This function returns error if the pipeline is interrupted before the stage finishes.
// The error is obtained internally by calling the `context.Err()` function.
//
// The result is in a random order.
//
// This function is useful when you need to remove all the duplicate elements from the stage channel.
//
// Only the comparable types are supported.
//
// This is a final stage function, which means that it leads to the end of the pipeline.
// If you need to set up the pipeline, use functions from the [pipeline] package.
// If you need to make more transformations, use functions from the [transform] package instead.
//
// If you need to count the distinct elements, use [DistinctCount] instead.
// If you need the async version of this function, use the asyncaggregate.Distinct instead.
func Distinct[In comparable](prevStage stages.Stage[In], confs ...configs.StageConfig) (*[]In, error) {
	once := &sync.Once{}

	return aggregate(
		prevStage,
		func(aggrRes types.AggregationWithCache[In], in In) types.AggregationWithCache[In] {
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

// DistinctCount is a sync aggregation function that counts all the distinct elements from the stage channel.
// This function returns error if the pipeline is interrupted before the stage finishes.
// The error is obtained internally by calling the `context.Err()` function.
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
// If you need the async version of this function, use the asyncaggregate.DistinctCount instead.
func DistinctCount[In comparable](prevStage stages.Stage[In], confs ...configs.StageConfig) (*int64, error) {
	once := &sync.Once{}

	return aggregate(
		prevStage,
		func(aggrRes types.AggregationWithCacheAndCounter[In], in In) types.AggregationWithCacheAndCounter[In] {
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

// aggregate is a generic sync aggregation function that aggregates all the elements from the stage channel using the given functions.
// This function returns error if the pipeline is interrupted before the stage finishes.
func aggregate[In, Aggr, Res any](prevStage stages.Stage[In], aggFunc func(aggrRes Aggr, in In) Aggr, resFunc func(aggrRes Aggr) Res, confs ...configs.StageConfig) (*Res, error) {
	validate(prevStage)

	inChan := prevStage.Chan
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

	var result Aggr

	running := true
	for running {
		select {
		case in, ok := <-inChan:
			if ok {
				localLogger.Debug(stageIdAsString + "input received")
				result = aggFunc(result, in)
				localLogger.Debug(stageIdAsString + "input processed")
			} else {
				localLogger.Debug(stageIdAsString + "input channel closed")
				running = false
			}
		case <-prevStage.Context().Done():
			localLogger.Debug(stageIdAsString + "context done signal received")
			utils.StopSafely(timeoutTimer)
			return nil, prevStage.Context().Err()
		}
	}

	utils.StopSafely(timeoutTimer)

	res := resFunc(result)
	prevStage.SetPipelineStatus(statuses.Done)

	localLogger.Info(stageIdAsString + "finished")

	return &res, nil
}

func validate[In any](prevStage stages.Stage[In]) {
	if prevStage.Starter != nil {
		prevStage.Logger.Error("Sync aggregation doesn't support manual delayed start - use async aggregation from the [asyncaggregate] package instead")
		panic("Sync aggregation doesn't support manual delayed start - use async aggregation from the [asyncaggregate] package instead")
	}
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
