package main

import (
	"fmt"
	"github.com/n0rdy/pippin/configs"
	"github.com/n0rdy/pippin/pipeline"
	"github.com/n0rdy/pippin/stages/aggregate"
	"github.com/n0rdy/pippin/stages/asyncaggregate"
	"github.com/n0rdy/pippin/stages/transform"
	"github.com/n0rdy/pippin/types"
	"github.com/n0rdy/pippin/types/statuses"
	"github.com/n0rdy/pippin/utils"
	"go.uber.org/goleak"
	"strconv"
	"testing"
	"time"
)

// tests goroutines memory leaks
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func TestFromSlice_AllPossibleTransformations_Sum_NoConfigs_Success(t *testing.T) {
	p := pipeline.FromSlice([]string{"1", "a", "2", "-3", "4", "5", "b"})

	if p.Status != statuses.Running {
		t.Errorf("expected status to be Running, got %s", p.Status.String())
	}

	atoiStage := transform.MapWithError(
		p.InitStage,
		func(input string) (int, error) {
			return strconv.Atoi(input)
		},
		func(err error) {
			fmt.Println(err)
		},
	)
	// 1, 2, -3, 4, 5

	oddNumsStage := transform.Filter(atoiStage, func(input int) bool {
		return input%2 != 0
	})
	// 1, -3, 5

	multipliedByTwoStage := transform.Map(oddNumsStage, func(input int) int {
		return input * 2
	})
	// 2, -6, 10

	toMatrixStage := transform.MapWithErrorMapper(
		multipliedByTwoStage,
		func(input int) ([]int, error) {
			if input < 0 {
				return nil, fmt.Errorf("negative number %d", input)
			}

			res := make([]int, input)
			for i := 0; i < input; i++ {
				res[i] = input * i
			}
			return res, nil
		},
		func(err error) []int {
			return []int{42}
		},
	)
	// [0, 2], [42], [0, 10, 20, 30, 40, 50, 60, 70, 80, 90]

	plusOneStage := transform.FlatMapWithError(
		toMatrixStage,
		func(input int) ([]int, error) {
			if input == 0 {
				return nil, fmt.Errorf("zero")
			}

			return []int{input + 1}, nil
		},
		func(err error) {
			fmt.Println(err)
		},
	)
	// [3], [43], [11], [21], [31], [41], [51], [61], [71], [81], [91]

	greaterThan42Stage := transform.FlatMapWithErrorMapper(
		plusOneStage,
		func(input int) ([]int, error) {
			if input <= 42 {
				return nil, fmt.Errorf("42")
			}
			return []int{input}, nil
		},
		func(err error) []int {
			return []int{0}
		},
	)
	// [0], [43], [0], [0], [0], [0], [51], [61], [71], [81], [91]

	flattenedStage := transform.FlatMap(greaterThan42Stage, func(input int) int {
		return input
	})
	// [0, 43, 0, 0, 0, 0, 51, 61, 71, 81, 91]

	sum, err := aggregate.Sum(flattenedStage)
	// 398

	if err != nil {
		t.Errorf("expected no error, got %v", err)
	} else {
		if *sum != 398 {
			t.Errorf("expected sum to be 398, got %d", *sum)
		}
	}

	// to sync with the pipeline
	time.Sleep(1 * time.Second)

	if p.Status != statuses.Done {
		t.Errorf("expected status to be done, got %s", p.Status.String())
	}
}

func TestFromSlice_AllPossibleTransformations_Sum_ManualStart_Success(t *testing.T) {
	p := pipeline.FromSlice(
		[]string{"1", "a", "2", "-3", "4", "5", "b"},
		configs.PipelineConfig{
			ManualStart: true,
		},
	)

	if p.Status != statuses.Pending {
		t.Errorf("expected status to be Pending, got %s", p.Status.String())
	}

	atoiStage := transform.MapWithError(
		p.InitStage,
		func(input string) (int, error) {
			return strconv.Atoi(input)
		},
		func(err error) {
			fmt.Println(err)
		},
	)
	// 1, 2, -3, 4, 5

	oddNumsStage := transform.Filter(atoiStage, func(input int) bool {
		return input%2 != 0
	})
	// 1, -3, 5

	multipliedByTwoStage := transform.Map(oddNumsStage, func(input int) int {
		return input * 2
	})
	// 2, -6, 10

	toMatrixStage := transform.MapWithErrorMapper(
		multipliedByTwoStage,
		func(input int) ([]int, error) {
			if input < 0 {
				return nil, fmt.Errorf("negative number %d", input)
			}

			res := make([]int, input)
			for i := 0; i < input; i++ {
				res[i] = input * i
			}
			return res, nil
		},
		func(err error) []int {
			return []int{42}
		},
	)
	// [0, 2], [42], [0, 10, 20, 30, 40, 50, 60, 70, 80, 90]

	plusOneStage := transform.FlatMapWithError(
		toMatrixStage,
		func(input int) ([]int, error) {
			if input == 0 {
				return nil, fmt.Errorf("zero")
			}

			return []int{input + 1}, nil
		},
		func(err error) {
			fmt.Println(err)
		},
	)
	// [3], [43], [11], [21], [31], [41], [51], [61], [71], [81], [91]

	greaterThan42Stage := transform.FlatMapWithErrorMapper(
		plusOneStage,
		func(input int) ([]int, error) {
			if input <= 42 {
				return nil, fmt.Errorf("42")
			}
			return []int{input}, nil
		},
		func(err error) []int {
			return []int{0}
		},
	)
	// [0], [43], [0], [0], [0], [0], [51], [61], [71], [81], [91]

	flattenedStage := transform.FlatMap(greaterThan42Stage, func(input int) int {
		return input
	})
	// [0, 43, 0, 0, 0, 0, 51, 61, 71, 81, 91]

	futureSum := asyncaggregate.Sum(flattenedStage)
	// 398

	if p.Status != statuses.Pending {
		t.Errorf("expected status to be Pending, got %s", p.Status.String())
	}

	p.Start()

	if p.Status != statuses.Running {
		t.Errorf("expected status to be Running, got %s", p.Status.String())
	}

	sum, err := futureSum.Get()
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	} else {
		if *sum != 398 {
			t.Errorf("expected sum to be 398, got %d", *sum)
		}
	}

	// to sync with the pipeline
	time.Sleep(1 * time.Second)

	if p.Status != statuses.Done {
		t.Errorf("expected status to be done, got %s", p.Status.String())
	}
}

func TestFromSlice_AllPossibleTransformations_Sum_ManualStart_InterruptedBeforeStart_Success(t *testing.T) {
	p := pipeline.FromSlice(
		[]string{"1", "a", "2", "-3", "4", "5", "b"},
		configs.PipelineConfig{
			ManualStart: true,
		},
	)

	if p.Status != statuses.Pending {
		t.Errorf("expected status to be Pending, got %s", p.Status.String())
	}

	atoiStage := transform.MapWithError(
		p.InitStage,
		func(input string) (int, error) {
			return strconv.Atoi(input)
		},
		func(err error) {
			fmt.Println(err)
		},
	)
	// 1, 2, -3, 4, 5

	oddNumsStage := transform.Filter(atoiStage, func(input int) bool {
		return input%2 != 0
	})
	// 1, -3, 5

	multipliedByTwoStage := transform.Map(oddNumsStage, func(input int) int {
		return input * 2
	})
	// 2, -6, 10

	toMatrixStage := transform.MapWithErrorMapper(
		multipliedByTwoStage,
		func(input int) ([]int, error) {
			if input < 0 {
				return nil, fmt.Errorf("negative number %d", input)
			}

			res := make([]int, input)
			for i := 0; i < input; i++ {
				res[i] = input * i
			}
			return res, nil
		},
		func(err error) []int {
			return []int{42}
		},
	)
	// [0, 2], [42], [0, 10, 20, 30, 40, 50, 60, 70, 80, 90]

	plusOneStage := transform.FlatMapWithError(
		toMatrixStage,
		func(input int) ([]int, error) {
			if input == 0 {
				return nil, fmt.Errorf("zero")
			}

			return []int{input + 1}, nil
		},
		func(err error) {
			fmt.Println(err)
		},
	)
	// [3], [43], [11], [21], [31], [41], [51], [61], [71], [81], [91]

	greaterThan42Stage := transform.FlatMapWithErrorMapper(
		plusOneStage,
		func(input int) ([]int, error) {
			if input <= 42 {
				return nil, fmt.Errorf("42")
			}
			return []int{input}, nil
		},
		func(err error) []int {
			return []int{0}
		},
	)
	// [0], [43], [0], [0], [0], [0], [51], [61], [71], [81], [91]

	flattenedStage := transform.FlatMap(greaterThan42Stage, func(input int) int {
		return input
	})
	// [0, 43, 0, 0, 0, 0, 51, 61, 71, 81, 91]

	futureSum := asyncaggregate.Sum(flattenedStage)
	// 398

	if p.Status != statuses.Pending {
		t.Errorf("expected status to be Pending, got %s", p.Status.String())
	}

	p.Interrupt()

	time.Sleep(100 * time.Millisecond)
	if p.Status != statuses.Interrupted {
		t.Errorf("expected status to be Interrupted, got %s", p.Status.String())
	}

	_, err := futureSum.Get()
	if err == nil {
		t.Errorf("expected error, got nil")
	} else if err.Error() != "context canceled" {
		t.Errorf("expected error to be 'context canceled', got %s", err.Error())
	}

	// to sync with the pipeline
	time.Sleep(1 * time.Second)

	if p.Status != statuses.Interrupted {
		t.Errorf("expected status to be interrupted, got %s", p.Status.String())
	}
}

func TestFromSlice_AllPossibleTransformations_Sum_ManualStart_InterruptedAfterStart_Success(t *testing.T) {
	p := pipeline.FromSlice(
		[]string{"1", "a", "2", "-3", "4", "5", "b"},
		configs.PipelineConfig{
			ManualStart: true,
		},
	)

	if p.Status != statuses.Pending {
		t.Errorf("expected status to be Pending, got %s", p.Status.String())
	}

	atoiStage := transform.MapWithError(
		p.InitStage,
		func(input string) (int, error) {
			time.Sleep(500 * time.Millisecond)

			return strconv.Atoi(input)
		},
		func(err error) {
			fmt.Println(err)
		},
	)
	// 1, 2, -3, 4, 5

	oddNumsStage := transform.Filter(atoiStage, func(input int) bool {
		return input%2 != 0
	})
	// 1, -3, 5

	multipliedByTwoStage := transform.Map(oddNumsStage, func(input int) int {
		return input * 2
	})
	// 2, -6, 10

	toMatrixStage := transform.MapWithErrorMapper(
		multipliedByTwoStage,
		func(input int) ([]int, error) {
			// to simulate a long-running stage in order to have time between the start and the interrupt
			time.Sleep(800 * time.Millisecond)

			if input < 0 {
				return nil, fmt.Errorf("negative number %d", input)
			}

			res := make([]int, input)
			for i := 0; i < input; i++ {
				res[i] = input * i
			}
			return res, nil
		},
		func(err error) []int {
			return []int{42}
		},
	)
	// [0, 2], [42], [0, 10, 20, 30, 40, 50, 60, 70, 80, 90]

	plusOneStage := transform.FlatMapWithError(
		toMatrixStage,
		func(input int) ([]int, error) {
			if input == 0 {
				return nil, fmt.Errorf("zero")
			}

			return []int{input + 1}, nil
		},
		func(err error) {
			fmt.Println(err)
		},
	)
	// [3], [43], [11], [21], [31], [41], [51], [61], [71], [81], [91]

	greaterThan42Stage := transform.FlatMapWithErrorMapper(
		plusOneStage,
		func(input int) ([]int, error) {
			// to simulate a long-running stage in order to have time between the start and the interrupt
			time.Sleep(200 * time.Millisecond)

			if input <= 42 {
				return nil, fmt.Errorf("42")
			}
			return []int{input}, nil
		},
		func(err error) []int {
			return []int{0}
		},
	)
	// [0], [43], [0], [0], [0], [0], [51], [61], [71], [81], [91]

	flattenedStage := transform.FlatMap(greaterThan42Stage, func(input int) int {
		return input
	})
	// [0, 43, 0, 0, 0, 0, 51, 61, 71, 81, 91]

	futureSum := asyncaggregate.Sum(flattenedStage)
	// 398

	if p.Status != statuses.Pending {
		t.Errorf("expected status to be Pending, got %s", p.Status.String())
	}

	p.Start()

	if p.Status != statuses.Running {
		t.Errorf("expected status to be Running, got %s", p.Status.String())
	}

	// to let pipeline work for some time
	time.Sleep(1 * time.Second)

	p.Interrupt()

	time.Sleep(100 * time.Millisecond)

	if p.Status != statuses.Interrupted {
		t.Errorf("expected status to be Interrupted, got %s", p.Status.String())
	}

	_, err := futureSum.Get()
	if err == nil {
		t.Errorf("expected error, got nil")
	} else if err.Error() != "context canceled" {
		t.Errorf("expected error to be 'context canceled', got %s", err.Error())
	}

	// to sync with the pipeline
	time.Sleep(1 * time.Second)

	if p.Status != statuses.Interrupted {
		t.Errorf("expected status to be interrupted, got %s", p.Status.String())
	}
}

func TestFromSlice_AllPossibleTransformations_Sum_PipelineTimeoutReached_Success(t *testing.T) {
	p := pipeline.FromSlice(
		[]string{"1", "a", "2", "-3", "4", "5", "b"},
		configs.PipelineConfig{
			TimeoutInMillis: 1000,
		},
	)

	if p.Status != statuses.Running {
		t.Errorf("expected status to be Pending, got %s", p.Status.String())
	}

	atoiStage := transform.MapWithError(
		p.InitStage,
		func(input string) (int, error) {
			return strconv.Atoi(input)
		},
		func(err error) {
			fmt.Println(err)
		},
	)
	// 1, 2, -3, 4, 5

	oddNumsStage := transform.Filter(atoiStage, func(input int) bool {
		return input%2 != 0
	})
	// 1, -3, 5

	multipliedByTwoStage := transform.Map(oddNumsStage, func(input int) int {
		return input * 2
	})
	// 2, -6, 10

	toMatrixStage := transform.MapWithErrorMapper(
		multipliedByTwoStage,
		func(input int) ([]int, error) {
			time.Sleep(2 * time.Second)
			if input < 0 {
				return nil, fmt.Errorf("negative number %d", input)
			}

			res := make([]int, input)
			for i := 0; i < input; i++ {
				res[i] = input * i
			}
			return res, nil
		},
		func(err error) []int {
			return []int{42}
		},
	)
	// [0, 2], [42], [0, 10, 20, 30, 40, 50, 60, 70, 80, 90]

	plusOneStage := transform.FlatMapWithError(
		toMatrixStage,
		func(input int) ([]int, error) {
			if input == 0 {
				return nil, fmt.Errorf("zero")
			}

			return []int{input + 1}, nil
		},
		func(err error) {
			fmt.Println(err)
		},
	)
	// [3], [43], [11], [21], [31], [41], [51], [61], [71], [81], [91]

	greaterThan42Stage := transform.FlatMapWithErrorMapper(
		plusOneStage,
		func(input int) ([]int, error) {
			if input <= 42 {
				return nil, fmt.Errorf("42")
			}
			return []int{input}, nil
		},
		func(err error) []int {
			return []int{0}
		},
	)
	// [0], [43], [0], [0], [0], [0], [51], [61], [71], [81], [91]

	flattenedStage := transform.FlatMap(greaterThan42Stage, func(input int) int {
		return input
	})
	// [0, 43, 0, 0, 0, 0, 51, 61, 71, 81, 91]

	_, err := aggregate.Sum(flattenedStage)

	if err == nil {
		t.Errorf("expected error, got nil")
	} else if err.Error() != "context canceled" {
		t.Errorf("expected error to be 'context canceled', got %s", err.Error())
	}

	time.Sleep(1 * time.Second)
	if p.Status != statuses.TimedOut {
		t.Errorf("expected status to be TimedOut, got %s", p.Status.String())
	}
}

func TestFromSlice_AllPossibleTransformations_Sum_StageTimeoutReached_Success(t *testing.T) {
	p := pipeline.FromSlice(
		[]string{"1", "a", "2", "-3", "4", "5", "b"},
		configs.PipelineConfig{
			TimeoutInMillis: 100000,
		},
	)

	if p.Status != statuses.Running {
		t.Errorf("expected status to be Pending, got %s", p.Status.String())
	}

	atoiStage := transform.MapWithError(
		p.InitStage,
		func(input string) (int, error) {
			return strconv.Atoi(input)
		},
		func(err error) {
			fmt.Println(err)
		},
	)
	// 1, 2, -3, 4, 5

	oddNumsStage := transform.Filter(atoiStage, func(input int) bool {
		return input%2 != 0
	})
	// 1, -3, 5

	multipliedByTwoStage := transform.Map(oddNumsStage, func(input int) int {
		return input * 2
	})
	// 2, -6, 10

	toMatrixStage := transform.MapWithErrorMapper(
		multipliedByTwoStage,
		func(input int) ([]int, error) {
			time.Sleep(1200 * time.Millisecond)
			if input < 0 {
				return nil, fmt.Errorf("negative number %d", input)
			}

			res := make([]int, input)
			for i := 0; i < input; i++ {
				res[i] = input * i
			}
			return res, nil
		},
		func(err error) []int {
			return []int{42}
		},
		configs.StageConfig{
			TimeoutInMillis: 1000,
		},
	)
	// [0, 2], [42], [0, 10, 20, 30, 40, 50, 60, 70, 80, 90]

	plusOneStage := transform.FlatMapWithError(
		toMatrixStage,
		func(input int) ([]int, error) {
			if input == 0 {
				return nil, fmt.Errorf("zero")
			}

			return []int{input + 1}, nil
		},
		func(err error) {
			fmt.Println(err)
		},
	)
	// [3], [43], [11], [21], [31], [41], [51], [61], [71], [81], [91]

	greaterThan42Stage := transform.FlatMapWithErrorMapper(
		plusOneStage,
		func(input int) ([]int, error) {
			if input <= 42 {
				return nil, fmt.Errorf("42")
			}
			return []int{input}, nil
		},
		func(err error) []int {
			return []int{0}
		},
	)
	// [0], [43], [0], [0], [0], [0], [51], [61], [71], [81], [91]

	flattenedStage := transform.FlatMap(greaterThan42Stage, func(input int) int {
		return input
	})
	// [0, 43, 0, 0, 0, 0, 51, 61, 71, 81, 91]

	_, err := aggregate.Sum(flattenedStage)

	if err == nil {
		t.Errorf("expected error, got nil")
	} else if err.Error() != "context canceled" {
		t.Errorf("expected error to be 'context canceled', got %s", err.Error())
	}

	time.Sleep(1 * time.Second)
	if p.Status != statuses.TimedOut {
		t.Errorf("expected status to be TimedOut, got %s", p.Status.String())
	}
}

func TestFromSlice_AllPossibleTransformations_Sum_PipelineRateLimiting_TooLowThreshold_Success(t *testing.T) {
	p := pipeline.FromSlice(
		[]string{"1", "a", "2", "-3", "4", "5", "b"},
		configs.PipelineConfig{
			MaxGoroutinesTotal: 5,
		},
	)

	if p.Status != statuses.Running {
		t.Errorf("expected status to be Running, got %s", p.Status.String())
	}

	atoiStage := transform.MapWithError(
		p.InitStage,
		func(input string) (int, error) {
			return strconv.Atoi(input)
		},
		func(err error) {
			fmt.Println(err)
		},
	)
	// 1, 2, -3, 4, 5

	oddNumsStage := transform.Filter(atoiStage, func(input int) bool {
		return input%2 != 0
	})
	// 1, -3, 5

	multipliedByTwoStage := transform.Map(oddNumsStage, func(input int) int {
		return input * 2
	})
	// 2, -6, 10

	toMatrixStage := transform.MapWithErrorMapper(
		multipliedByTwoStage,
		func(input int) ([]int, error) {
			if input < 0 {
				return nil, fmt.Errorf("negative number %d", input)
			}

			res := make([]int, input)
			for i := 0; i < input; i++ {
				res[i] = input * i
			}
			return res, nil
		},
		func(err error) []int {
			return []int{42}
		},
	)
	// [0, 2], [42], [0, 10, 20, 30, 40, 50, 60, 70, 80, 90]

	plusOneStage := transform.FlatMapWithError(
		toMatrixStage,
		func(input int) ([]int, error) {
			if input == 0 {
				return nil, fmt.Errorf("zero")
			}

			return []int{input + 1}, nil
		},
		func(err error) {
			fmt.Println(err)
		},
	)
	// [3], [43], [11], [21], [31], [41], [51], [61], [71], [81], [91]

	greaterThan42Stage := transform.FlatMapWithErrorMapper(
		plusOneStage,
		func(input int) ([]int, error) {
			if input <= 42 {
				return nil, fmt.Errorf("42")
			}
			return []int{input}, nil
		},
		func(err error) []int {
			return []int{0}
		},
	)
	// [0], [43], [0], [0], [0], [0], [51], [61], [71], [81], [91]

	flattenedStage := transform.FlatMap(greaterThan42Stage, func(input int) int {
		return input
	})
	// [0, 43, 0, 0, 0, 0, 51, 61, 71, 81, 91]

	sum, err := aggregate.Sum(flattenedStage)
	// 398

	if err != nil {
		t.Errorf("expected no error, got %v", err)
	} else {
		if *sum != 398 {
			t.Errorf("expected sum to be 398, got %d", *sum)
		}
	}

	// to sync with the pipeline
	time.Sleep(1 * time.Second)

	if p.Status != statuses.Done {
		t.Errorf("expected status to be done, got %s", p.Status.String())
	}
}

func TestFromSlice_AllPossibleTransformations_Sum_PipelineRateLimiting_HighThreshold_Success(t *testing.T) {
	p := pipeline.FromSlice(
		[]string{"1", "a", "2", "-3", "4", "5", "b"},
		configs.PipelineConfig{
			MaxGoroutinesTotal: 100,
		},
	)

	if p.Status != statuses.Running {
		t.Errorf("expected status to be Running, got %s", p.Status.String())
	}

	atoiStage := transform.MapWithError(
		p.InitStage,
		func(input string) (int, error) {
			return strconv.Atoi(input)
		},
		func(err error) {
			fmt.Println(err)
		},
	)
	// 1, 2, -3, 4, 5

	oddNumsStage := transform.Filter(atoiStage, func(input int) bool {
		return input%2 != 0
	})
	// 1, -3, 5

	multipliedByTwoStage := transform.Map(oddNumsStage, func(input int) int {
		return input * 2
	})
	// 2, -6, 10

	toMatrixStage := transform.MapWithErrorMapper(
		multipliedByTwoStage,
		func(input int) ([]int, error) {
			if input < 0 {
				return nil, fmt.Errorf("negative number %d", input)
			}

			res := make([]int, input)
			for i := 0; i < input; i++ {
				res[i] = input * i
			}
			return res, nil
		},
		func(err error) []int {
			return []int{42}
		},
	)
	// [0, 2], [42], [0, 10, 20, 30, 40, 50, 60, 70, 80, 90]

	plusOneStage := transform.FlatMapWithError(
		toMatrixStage,
		func(input int) ([]int, error) {
			if input == 0 {
				return nil, fmt.Errorf("zero")
			}

			return []int{input + 1}, nil
		},
		func(err error) {
			fmt.Println(err)
		},
	)
	// [3], [43], [11], [21], [31], [41], [51], [61], [71], [81], [91]

	greaterThan42Stage := transform.FlatMapWithErrorMapper(
		plusOneStage,
		func(input int) ([]int, error) {
			if input <= 42 {
				return nil, fmt.Errorf("42")
			}
			return []int{input}, nil
		},
		func(err error) []int {
			return []int{0}
		},
	)
	// [0], [43], [0], [0], [0], [0], [51], [61], [71], [81], [91]

	flattenedStage := transform.FlatMap(greaterThan42Stage, func(input int) int {
		return input
	})
	// [0, 43, 0, 0, 0, 0, 51, 61, 71, 81, 91]

	sum, err := aggregate.Sum(flattenedStage)
	// 398

	if err != nil {
		t.Errorf("expected no error, got %v", err)
	} else {
		if *sum != 398 {
			t.Errorf("expected sum to be 398, got %d", *sum)
		}
	}

	// to sync with the pipeline
	time.Sleep(1 * time.Second)

	if p.Status != statuses.Done {
		t.Errorf("expected status to be done, got %s", p.Status.String())
	}
}

func TestFromSlice_AllPossibleTransformations_Sum_PipelineAndStageRateLimiting_Success(t *testing.T) {
	p := pipeline.FromSlice(
		[]string{"1", "a", "2", "-3", "4", "5", "b"},
		configs.PipelineConfig{
			MaxGoroutinesTotal:    100,
			MaxGoroutinesPerStage: 1,
		},
	)

	if p.Status != statuses.Running {
		t.Errorf("expected status to be Running, got %s", p.Status.String())
	}

	atoiStage := transform.MapWithError(
		p.InitStage,
		func(input string) (int, error) {
			return strconv.Atoi(input)
		},
		func(err error) {
			fmt.Println(err)
		},
	)
	// 1, 2, -3, 4, 5

	oddNumsStage := transform.Filter(atoiStage, func(input int) bool {
		return input%2 != 0
	})
	// 1, -3, 5

	multipliedByTwoStage := transform.Map(oddNumsStage, func(input int) int {
		return input * 2
	})
	// 2, -6, 10

	toMatrixStage := transform.MapWithErrorMapper(
		multipliedByTwoStage,
		func(input int) ([]int, error) {
			if input < 0 {
				return nil, fmt.Errorf("negative number %d", input)
			}

			res := make([]int, input)
			for i := 0; i < input; i++ {
				res[i] = input * i
			}
			return res, nil
		},
		func(err error) []int {
			return []int{42}
		},
	)
	// [0, 2], [42], [0, 10, 20, 30, 40, 50, 60, 70, 80, 90]

	plusOneStage := transform.FlatMapWithError(
		toMatrixStage,
		func(input int) ([]int, error) {
			if input == 0 {
				return nil, fmt.Errorf("zero")
			}

			return []int{input + 1}, nil
		},
		func(err error) {
			fmt.Println(err)
		},
	)
	// [3], [43], [11], [21], [31], [41], [51], [61], [71], [81], [91]

	greaterThan42Stage := transform.FlatMapWithErrorMapper(
		plusOneStage,
		func(input int) ([]int, error) {
			if input <= 42 {
				return nil, fmt.Errorf("42")
			}
			return []int{input}, nil
		},
		func(err error) []int {
			return []int{0}
		},
	)
	// [0], [43], [0], [0], [0], [0], [51], [61], [71], [81], [91]

	flattenedStage := transform.FlatMap(greaterThan42Stage, func(input int) int {
		return input
	})
	// [0, 43, 0, 0, 0, 0, 51, 61, 71, 81, 91]

	sum, err := aggregate.Sum(flattenedStage)
	// 398

	if err != nil {
		t.Errorf("expected no error, got %v", err)
	} else {
		if *sum != 398 {
			t.Errorf("expected sum to be 398, got %d", *sum)
		}
	}

	// to sync with the pipeline
	time.Sleep(1 * time.Second)

	if p.Status != statuses.Done {
		t.Errorf("expected status to be done, got %s", p.Status.String())
	}
}

func TestFromSlice_AllPossibleTransformations_Sum_StageRateLimiting_PerStage_HighThreshold_Success(t *testing.T) {
	p := pipeline.FromSlice(
		[]string{"1", "a", "2", "-3", "4", "5", "b"},
		configs.PipelineConfig{
			MaxGoroutinesPerStage: 100,
		},
	)

	if p.Status != statuses.Running {
		t.Errorf("expected status to be Running, got %s", p.Status.String())
	}

	atoiStage := transform.MapWithError(
		p.InitStage,
		func(input string) (int, error) {
			return strconv.Atoi(input)
		},
		func(err error) {
			fmt.Println(err)
		},
	)
	// 1, 2, -3, 4, 5

	oddNumsStage := transform.Filter(atoiStage, func(input int) bool {
		return input%2 != 0
	})
	// 1, -3, 5

	multipliedByTwoStage := transform.Map(oddNumsStage, func(input int) int {
		return input * 2
	})
	// 2, -6, 10

	toMatrixStage := transform.MapWithErrorMapper(
		multipliedByTwoStage,
		func(input int) ([]int, error) {
			if input < 0 {
				return nil, fmt.Errorf("negative number %d", input)
			}

			res := make([]int, input)
			for i := 0; i < input; i++ {
				res[i] = input * i
			}
			return res, nil
		},
		func(err error) []int {
			return []int{42}
		},
	)
	// [0, 2], [42], [0, 10, 20, 30, 40, 50, 60, 70, 80, 90]

	plusOneStage := transform.FlatMapWithError(
		toMatrixStage,
		func(input int) ([]int, error) {
			if input == 0 {
				return nil, fmt.Errorf("zero")
			}

			return []int{input + 1}, nil
		},
		func(err error) {
			fmt.Println(err)
		},
	)
	// [3], [43], [11], [21], [31], [41], [51], [61], [71], [81], [91]

	greaterThan42Stage := transform.FlatMapWithErrorMapper(
		plusOneStage,
		func(input int) ([]int, error) {
			if input <= 42 {
				return nil, fmt.Errorf("42")
			}
			return []int{input}, nil
		},
		func(err error) []int {
			return []int{0}
		},
	)
	// [0], [43], [0], [0], [0], [0], [51], [61], [71], [81], [91]

	flattenedStage := transform.FlatMap(greaterThan42Stage, func(input int) int {
		return input
	})
	// [0, 43, 0, 0, 0, 0, 51, 61, 71, 81, 91]

	sum, err := aggregate.Sum(flattenedStage)
	// 398

	if err != nil {
		t.Errorf("expected no error, got %v", err)
	} else {
		if *sum != 398 {
			t.Errorf("expected sum to be 398, got %d", *sum)
		}
	}

	// to sync with the pipeline
	time.Sleep(1 * time.Second)

	if p.Status != statuses.Done {
		t.Errorf("expected status to be done, got %s", p.Status.String())
	}
}

func TestFromSlice_AllPossibleTransformations_Sum_StageRateLimiting_PerStage_LowThreshold_Success(t *testing.T) {
	p := pipeline.FromSlice(
		[]string{"1", "a", "2", "-3", "4", "5", "b"},
		configs.PipelineConfig{
			MaxGoroutinesPerStage: 1,
		},
	)

	if p.Status != statuses.Running {
		t.Errorf("expected status to be Running, got %s", p.Status.String())
	}

	atoiStage := transform.MapWithError(
		p.InitStage,
		func(input string) (int, error) {
			return strconv.Atoi(input)
		},
		func(err error) {
			fmt.Println(err)
		},
	)
	// 1, 2, -3, 4, 5

	oddNumsStage := transform.Filter(atoiStage, func(input int) bool {
		return input%2 != 0
	})
	// 1, -3, 5

	multipliedByTwoStage := transform.Map(oddNumsStage, func(input int) int {
		return input * 2
	})
	// 2, -6, 10

	toMatrixStage := transform.MapWithErrorMapper(
		multipliedByTwoStage,
		func(input int) ([]int, error) {
			if input < 0 {
				return nil, fmt.Errorf("negative number %d", input)
			}

			res := make([]int, input)
			for i := 0; i < input; i++ {
				res[i] = input * i
			}
			return res, nil
		},
		func(err error) []int {
			return []int{42}
		},
	)
	// [0, 2], [42], [0, 10, 20, 30, 40, 50, 60, 70, 80, 90]

	plusOneStage := transform.FlatMapWithError(
		toMatrixStage,
		func(input int) ([]int, error) {
			if input == 0 {
				return nil, fmt.Errorf("zero")
			}

			return []int{input + 1}, nil
		},
		func(err error) {
			fmt.Println(err)
		},
	)
	// [3], [43], [11], [21], [31], [41], [51], [61], [71], [81], [91]

	greaterThan42Stage := transform.FlatMapWithErrorMapper(
		plusOneStage,
		func(input int) ([]int, error) {
			if input <= 42 {
				return nil, fmt.Errorf("42")
			}
			return []int{input}, nil
		},
		func(err error) []int {
			return []int{0}
		},
	)
	// [0], [43], [0], [0], [0], [0], [51], [61], [71], [81], [91]

	flattenedStage := transform.FlatMap(greaterThan42Stage, func(input int) int {
		return input
	})
	// [0, 43, 0, 0, 0, 0, 51, 61, 71, 81, 91]

	sum, err := aggregate.Sum(flattenedStage)
	// 398

	if err != nil {
		t.Errorf("expected no error, got %v", err)
	} else {
		if *sum != 398 {
			t.Errorf("expected sum to be 398, got %d", *sum)
		}
	}

	// to sync with the pipeline
	time.Sleep(1 * time.Second)

	if p.Status != statuses.Done {
		t.Errorf("expected status to be done, got %s", p.Status.String())
	}
}

func TestFromSlice_AllPossibleTransformations_Sum_StageRateLimiting_ForStage_Success(t *testing.T) {
	p := pipeline.FromSlice(
		[]string{"1", "a", "2", "-3", "4", "5", "b"},
	)

	if p.Status != statuses.Running {
		t.Errorf("expected status to be Running, got %s", p.Status.String())
	}

	atoiStage := transform.MapWithError(
		p.InitStage,
		func(input string) (int, error) {
			return strconv.Atoi(input)
		},
		func(err error) {
			fmt.Println(err)
		},
		configs.StageConfig{
			MaxGoroutines: 1,
		},
	)
	// 1, 2, -3, 4, 5

	oddNumsStage := transform.Filter(atoiStage, func(input int) bool {
		return input%2 != 0
	})
	// 1, -3, 5

	multipliedByTwoStage := transform.Map(oddNumsStage, func(input int) int {
		return input * 2
	})
	// 2, -6, 10

	toMatrixStage := transform.MapWithErrorMapper(
		multipliedByTwoStage,
		func(input int) ([]int, error) {
			if input < 0 {
				return nil, fmt.Errorf("negative number %d", input)
			}

			res := make([]int, input)
			for i := 0; i < input; i++ {
				res[i] = input * i
			}
			return res, nil
		},
		func(err error) []int {
			return []int{42}
		},
		configs.StageConfig{
			MaxGoroutines: 10,
		},
	)
	// [0, 2], [42], [0, 10, 20, 30, 40, 50, 60, 70, 80, 90]

	plusOneStage := transform.FlatMapWithError(
		toMatrixStage,
		func(input int) ([]int, error) {
			if input == 0 {
				return nil, fmt.Errorf("zero")
			}

			return []int{input + 1}, nil
		},
		func(err error) {
			fmt.Println(err)
		},
	)
	// [3], [43], [11], [21], [31], [41], [51], [61], [71], [81], [91]

	greaterThan42Stage := transform.FlatMapWithErrorMapper(
		plusOneStage,
		func(input int) ([]int, error) {
			if input <= 42 {
				return nil, fmt.Errorf("42")
			}
			return []int{input}, nil
		},
		func(err error) []int {
			return []int{0}
		},
	)
	// [0], [43], [0], [0], [0], [0], [51], [61], [71], [81], [91]

	flattenedStage := transform.FlatMap(greaterThan42Stage, func(input int) int {
		return input
	})
	// [0, 43, 0, 0, 0, 0, 51, 61, 71, 81, 91]

	sum, err := aggregate.Sum(flattenedStage)
	// 398

	if err != nil {
		t.Errorf("expected no error, got %v", err)
	} else {
		if *sum != 398 {
			t.Errorf("expected sum to be 398, got %d", *sum)
		}
	}

	// to sync with the pipeline
	time.Sleep(1 * time.Second)

	if p.Status != statuses.Done {
		t.Errorf("expected status to be done, got %s", p.Status.String())
	}
}

func TestFromSlice_AllPossibleTransformations_Sum_StageRateLimiting_PerStage_OverwrittenForStage_Success(t *testing.T) {
	p := pipeline.FromSlice(
		[]string{"1", "a", "2", "-3", "4", "5", "b"},
		configs.PipelineConfig{
			MaxGoroutinesPerStage: 1,
		},
	)

	if p.Status != statuses.Running {
		t.Errorf("expected status to be Running, got %s", p.Status.String())
	}

	atoiStage := transform.MapWithError(
		p.InitStage,
		func(input string) (int, error) {
			return strconv.Atoi(input)
		},
		func(err error) {
			fmt.Println(err)
		},
		configs.StageConfig{
			MaxGoroutines: 10,
		},
	)
	// 1, 2, -3, 4, 5

	oddNumsStage := transform.Filter(atoiStage, func(input int) bool {
		return input%2 != 0
	})
	// 1, -3, 5

	multipliedByTwoStage := transform.Map(oddNumsStage, func(input int) int {
		return input * 2
	})
	// 2, -6, 10

	toMatrixStage := transform.MapWithErrorMapper(
		multipliedByTwoStage,
		func(input int) ([]int, error) {
			if input < 0 {
				return nil, fmt.Errorf("negative number %d", input)
			}

			res := make([]int, input)
			for i := 0; i < input; i++ {
				res[i] = input * i
			}
			return res, nil
		},
		func(err error) []int {
			return []int{42}
		},
	)
	// [0, 2], [42], [0, 10, 20, 30, 40, 50, 60, 70, 80, 90]

	plusOneStage := transform.FlatMapWithError(
		toMatrixStage,
		func(input int) ([]int, error) {
			if input == 0 {
				return nil, fmt.Errorf("zero")
			}

			return []int{input + 1}, nil
		},
		func(err error) {
			fmt.Println(err)
		},
	)
	// [3], [43], [11], [21], [31], [41], [51], [61], [71], [81], [91]

	greaterThan42Stage := transform.FlatMapWithErrorMapper(
		plusOneStage,
		func(input int) ([]int, error) {
			if input <= 42 {
				return nil, fmt.Errorf("42")
			}
			return []int{input}, nil
		},
		func(err error) []int {
			return []int{0}
		},
	)
	// [0], [43], [0], [0], [0], [0], [51], [61], [71], [81], [91]

	flattenedStage := transform.FlatMap(greaterThan42Stage, func(input int) int {
		return input
	})
	// [0, 43, 0, 0, 0, 0, 51, 61, 71, 81, 91]

	sum, err := aggregate.Sum(flattenedStage)
	// 398

	if err != nil {
		t.Errorf("expected no error, got %v", err)
	} else {
		if *sum != 398 {
			t.Errorf("expected sum to be 398, got %d", *sum)
		}
	}

	// to sync with the pipeline
	time.Sleep(1 * time.Second)

	if p.Status != statuses.Done {
		t.Errorf("expected status to be done, got %s", p.Status.String())
	}
}

func TestFromSlice_Map_Sync_SumComplexType_Success(t *testing.T) {
	p := pipeline.FromSlice(
		[]string{"1", "2", "-3", "4", "5"},
	)

	atoiStage := transform.Map(
		p.InitStage,
		func(input string) complex64 {
			i, _ := strconv.Atoi(input)
			return complex(float32(i), float32(i))
		},
	)

	sum, err := aggregate.SumComplexType(atoiStage)

	if err != nil {
		t.Errorf("expected no error, got %v", err)
	} else {
		if *sum != complex(9, 9) {
			t.Error("expected sum to be 9, got ", *sum)
		}
	}

	time.Sleep(1 * time.Second)
	if p.Status != statuses.Done {
		t.Errorf("expected status to be Done, got %s", p.Status.String())
	}
}

func TestFromSlice_Map_Sync_Avg_Success(t *testing.T) {
	p := pipeline.FromSlice(
		[]string{"1", "2", "-3", "4", "5"},
	)

	atoiStage := transform.Map(
		p.InitStage,
		func(input string) int {
			i, _ := strconv.Atoi(input)
			return i
		},
	)

	avg, err := aggregate.Avg(atoiStage)

	if err != nil {
		t.Errorf("expected no error, got %v", err)
	} else {
		if *avg != 1.8 {
			t.Error("expected avg to be 1.8, got ", *avg)
		}
	}

	time.Sleep(1 * time.Second)
	if p.Status != statuses.Done {
		t.Errorf("expected status to be Done, got %s", p.Status.String())
	}
}

func TestFromSlice_Map_Sync_AvgComplexType_Success(t *testing.T) {
	p := pipeline.FromSlice(
		[]string{"1", "2", "-3", "4", "5"},
	)

	atoiStage := transform.Map(
		p.InitStage,
		func(input string) complex64 {
			i, _ := strconv.Atoi(input)
			return complex(float32(i), float32(i))
		},
	)

	avg, err := aggregate.AvgComplexType(atoiStage)

	if err != nil {
		t.Errorf("expected no error, got %v", err)
	} else {
		if real(*avg) != 1.8 {
			t.Error("expected avg to be 1.8, got ", *avg)
		}
	}

	time.Sleep(1 * time.Second)
	if p.Status != statuses.Done {
		t.Errorf("expected status to be Done, got %s", p.Status.String())
	}
}

func TestFromSlice_Map_Sync_Max_Success(t *testing.T) {
	p := pipeline.FromSlice(
		[]string{"1", "2", "-3", "4", "5"},
	)

	atoiStage := transform.Map(
		p.InitStage,
		func(input string) int {
			i, _ := strconv.Atoi(input)
			return i
		},
	)

	m, err := aggregate.Max(atoiStage)

	if err != nil {
		t.Errorf("expected no error, got %v", err)
	} else {
		if *m != 5 {
			t.Error("expected max to be 5, got ", *m)
		}
	}

	time.Sleep(1 * time.Second)
	if p.Status != statuses.Done {
		t.Errorf("expected status to be Done, got %s", p.Status.String())
	}
}

func TestFromSlice_Map_Sync_Min_Success(t *testing.T) {
	p := pipeline.FromSlice(
		[]string{"1", "2", "-3", "4", "5"},
	)

	atoiStage := transform.Map(
		p.InitStage,
		func(input string) int {
			i, _ := strconv.Atoi(input)
			return i
		},
	)

	m, err := aggregate.Min(atoiStage)

	if err != nil {
		t.Errorf("expected no error, got %v", err)
	} else {
		if *m != -3 {
			t.Error("expected min to be -3, got ", *m)
		}
	}

	time.Sleep(1 * time.Second)
	if p.Status != statuses.Done {
		t.Errorf("expected status to be Done, got %s", p.Status.String())
	}
}

func TestFromSlice_Map_Sync_Count_Success(t *testing.T) {
	p := pipeline.FromSlice(
		[]string{"1", "2", "-3", "4", "5"},
	)

	atoiStage := transform.Map(
		p.InitStage,
		func(input string) int {
			i, _ := strconv.Atoi(input)
			return i
		},
	)

	count, err := aggregate.Count(atoiStage)

	if err != nil {
		t.Errorf("expected no error, got %v", err)
	} else {
		if *count != 5 {
			t.Error("expected count to be 5, got ", *count)
		}
	}

	time.Sleep(1 * time.Second)
	if p.Status != statuses.Done {
		t.Errorf("expected status to be Done, got %s", p.Status.String())
	}
}

func TestFromSlice_Map_Sync_Sort_Success(t *testing.T) {
	expected := []int{-3, 1, 2, 4, 5}

	p := pipeline.FromSlice(
		[]string{"1", "2", "-3", "4", "5"},
	)

	atoiStage := transform.Map(
		p.InitStage,
		func(input string) int {
			i, _ := strconv.Atoi(input)
			return i
		},
	)

	sorted, err := aggregate.Sort(atoiStage)

	if err != nil {
		t.Errorf("expected no error, got %v", err)
	} else {
		sortedVal := *sorted
		if len(sortedVal) != 5 {
			t.Error("expected sorted to be of len 5, got ", len(sortedVal))
		} else {
			if !utils.SlicesEqual(sortedVal, expected) {
				t.Error("expected sorted to be [-3, 1, 2, 4, 5], got ", sortedVal)
			}
		}
	}

	time.Sleep(1 * time.Second)
	if p.Status != statuses.Done {
		t.Errorf("expected status to be Done, got %s", p.Status.String())
	}
}

func TestFromSlice_Map_Sync_SortDesc_Success(t *testing.T) {
	expected := []int{5, 4, 2, 1, -3}

	p := pipeline.FromSlice(
		[]string{"1", "2", "-3", "4", "5"},
	)

	atoiStage := transform.Map(
		p.InitStage,
		func(input string) int {
			i, _ := strconv.Atoi(input)
			return i
		},
	)

	sorted, err := aggregate.SortDesc(atoiStage)

	if err != nil {
		t.Errorf("expected no error, got %v", err)
	} else {
		sortedVal := *sorted
		if len(sortedVal) != 5 {
			t.Error("expected sorted to be of len 5, got ", len(sortedVal))
		} else {
			if !utils.SlicesEqual(sortedVal, expected) {
				t.Error("expected sorted to be [5, 4, 2, 1, -3], got ", sortedVal)
			}
		}
	}

	time.Sleep(1 * time.Second)
	if p.Status != statuses.Done {
		t.Errorf("expected status to be Done, got %s", p.Status.String())
	}
}

func TestFromSlice_Map_Sync_GroupBy_Success(t *testing.T) {
	expected := map[string][]int{
		"even": {2, 4},
		"odd":  {1, -3, 5},
	}

	p := pipeline.FromSlice(
		[]string{"1", "2", "-3", "4", "5"},
	)

	atoiStage := transform.Map(
		p.InitStage,
		func(input string) int {
			i, _ := strconv.Atoi(input)
			return i
		},
	)

	grouped, err := aggregate.GroupBy(atoiStage, func(input int) string {
		if input%2 == 0 {
			return "even"
		} else {
			return "odd"
		}
	})

	if err != nil {
		t.Errorf("expected no error, got %v", err)
	} else {
		groupedVal := *grouped
		if len(groupedVal) != 2 {
			t.Error("expected grouped to be of len 2, got ", len(groupedVal))
		} else {
			if !utils.MultiMapsEqual(groupedVal, expected) {
				t.Errorf("expected grouped to be %v, got %v", expected, groupedVal)
			}
		}
	}

	time.Sleep(1 * time.Second)
	if p.Status != statuses.Done {
		t.Errorf("expected status to be Done, got %s", p.Status.String())
	}
}

func TestFromSlice_Map_Sync_Reduce_Success(t *testing.T) {
	p := pipeline.FromSlice(
		[]string{"1", "2", "-3", "4", "5"},
	)

	atoiStage := transform.Map(
		p.InitStage,
		func(input string) int {
			i, _ := strconv.Atoi(input)
			return i
		},
	)

	reduced, err := aggregate.Reduce(atoiStage, func(acc int, input int) int {
		return acc + input
	})

	if err != nil {
		t.Errorf("expected no error, got %v", err)
	} else if *reduced != 9 {
		t.Errorf("expected reduced to be 9, got %d", *reduced)
	}

	time.Sleep(1 * time.Second)
	if p.Status != statuses.Done {
		t.Errorf("expected status to be Done, got %s", p.Status.String())
	}
}

func TestFromSlice_Map_Sync_AsSlice_Success(t *testing.T) {
	expected := []int{1, 2, -3, 4, 5}

	p := pipeline.FromSlice(
		[]string{"1", "2", "-3", "4", "5"},
	)

	atoiStage := transform.Map(
		p.InitStage,
		func(input string) int {
			i, _ := strconv.Atoi(input)
			return i
		},
	)

	s, err := aggregate.AsSlice(atoiStage)

	if err != nil {
		t.Errorf("expected no error, got %v", err)
	} else {
		sVal := *s
		if len(sVal) != 5 {
			t.Error("expected slice to be of len 5, got ", len(sVal))
		} else {
			if !utils.SlicesEqualIgnoreOrder(sVal, expected) {
				t.Error("expected slice to be [1, 2, -3, 4, 5], got ", sVal)
			}
		}
	}

	time.Sleep(1 * time.Second)
	if p.Status != statuses.Done {
		t.Errorf("expected status to be Done, got %s", p.Status.String())
	}
}

func TestFromSlice_Map_Sync_AsMap_Success(t *testing.T) {
	expected := map[string]int{
		"1":  1,
		"2":  2,
		"-3": -3,
		"4":  4,
		"5":  5,
	}

	p := pipeline.FromSlice(
		[]string{"1", "2", "-3", "4", "5"},
	)

	m, err := aggregate.AsMap(
		p.InitStage,
		func(input string) types.Tuple[string, int] {
			i, _ := strconv.Atoi(input)
			return types.Tuple[string, int]{
				First:  input,
				Second: i,
			}
		})

	if err != nil {
		t.Errorf("expected no error, got %v", err)
	} else {
		mVal := *m
		if len(mVal) != 5 {
			t.Error("expected map to be of len 5, got ", len(mVal))
		} else {
			if !utils.MapsEqual(mVal, expected) {
				t.Errorf("expected map to be %v, got %v", expected, mVal)
			}
		}
	}

	time.Sleep(1 * time.Second)
	if p.Status != statuses.Done {
		t.Errorf("expected status to be Done, got %s", p.Status.String())
	}
}

func TestFromSlice_Map_Sync_AsMultiMap_Success(t *testing.T) {
	expected := map[string][]int{
		"even": {2, 4},
		"odd":  {1, -3, 5},
	}

	p := pipeline.FromSlice(
		[]string{"1", "2", "-3", "4", "5"},
	)

	atoiStage := transform.Map(
		p.InitStage,
		func(input string) int {
			i, _ := strconv.Atoi(input)
			return i
		},
	)

	m, err := aggregate.AsMultiMap(atoiStage, func(input int) types.Tuple[string, int] {
		if input%2 == 0 {
			return types.Tuple[string, int]{
				First:  "even",
				Second: input,
			}
		} else {
			return types.Tuple[string, int]{
				First:  "odd",
				Second: input,
			}
		}
	})

	if err != nil {
		t.Errorf("expected no error, got %v", err)
	} else {
		mVal := *m
		if len(mVal) != 2 {
			t.Error("expected map to be of len 2, got ", len(mVal))
		} else {
			if !utils.MultiMapsEqual(mVal, expected) {
				t.Errorf("expected map to be %v, got %v", expected, mVal)
			}
		}
	}

	time.Sleep(1 * time.Second)
	if p.Status != statuses.Done {
		t.Errorf("expected status to be Done, got %s", p.Status.String())
	}
}

func TestFromSlice_Map_Sync_ForEach_Success(t *testing.T) {
	p := pipeline.FromSlice(
		[]string{"1", "2", "-3", "4", "5"},
	)

	atoiStage := transform.Map(
		p.InitStage,
		func(input string) int {
			i, _ := strconv.Atoi(input)
			return i
		},
	)

	err := aggregate.ForEach(atoiStage, func(i int) {
		fmt.Println(i)
	})

	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}

	time.Sleep(1 * time.Second)
	if p.Status != statuses.Done {
		t.Errorf("expected status to be Done, got %s", p.Status.String())
	}
}

func TestFromSlice_Map_Sync_Distinct_Success(t *testing.T) {
	expected := []int{1, 2, -3, 4, 5}

	p := pipeline.FromSlice(
		[]string{"1", "2", "-3", "4", "5", "1", "1", "1", "5", "-3"},
	)

	atoiStage := transform.Map(
		p.InitStage,
		func(input string) int {
			i, _ := strconv.Atoi(input)
			return i
		},
	)

	s, err := aggregate.Distinct(atoiStage)

	if err != nil {
		t.Errorf("expected no error, got %v", err)
	} else {
		sVal := *s
		if len(sVal) != 5 {
			t.Error("expected slice to be of len 5, got ", len(sVal))
		} else {
			if !utils.SlicesEqualIgnoreOrder(sVal, expected) {
				t.Error("expected slice to be [1, 2, -3, 4, 5], got ", sVal)
			}
		}
	}

	time.Sleep(1 * time.Second)
	if p.Status != statuses.Done {
		t.Errorf("expected status to be Done, got %s", p.Status.String())
	}
}

func TestFromSlice_Map_Sync_DistinctCount_Success(t *testing.T) {
	p := pipeline.FromSlice(
		[]string{"1", "2", "-3", "4", "5", "1", "1", "1", "5", "-3"},
	)

	atoiStage := transform.Map(
		p.InitStage,
		func(input string) int {
			i, _ := strconv.Atoi(input)
			return i
		},
	)

	uniqueCount, err := aggregate.DistinctCount(atoiStage)

	if err != nil {
		t.Errorf("expected no error, got %v", err)
	} else if *uniqueCount != 5 {
		t.Errorf("expected uniqueCount to be 5, got %d", *uniqueCount)
	}

	time.Sleep(1 * time.Second)
	if p.Status != statuses.Done {
		t.Errorf("expected status to be Done, got %s", p.Status.String())
	}
}

func TestFromSlice_Map_Async_SumComplexType_Success(t *testing.T) {
	p := pipeline.FromSlice(
		[]string{"1", "2", "-3", "4", "5"},
	)

	atoiStage := transform.Map(
		p.InitStage,
		func(input string) complex64 {
			i, _ := strconv.Atoi(input)
			return complex(float32(i), float32(i))
		},
	)

	sumFuture := asyncaggregate.SumComplexType(atoiStage)

	sum, err := sumFuture.Get()
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	} else {
		if *sum != complex(9, 9) {
			t.Error("expected sum to be 9, got ", *sum)
		}
	}

	time.Sleep(1 * time.Second)
	if p.Status != statuses.Done {
		t.Errorf("expected status to be Done, got %s", p.Status.String())
	}
}

func TestFromSlice_Map_Async_Avg_Success(t *testing.T) {
	p := pipeline.FromSlice(
		[]string{"1", "2", "-3", "4", "5"},
	)

	atoiStage := transform.Map(
		p.InitStage,
		func(input string) int {
			i, _ := strconv.Atoi(input)
			return i
		},
	)

	avgFuture := asyncaggregate.Avg(atoiStage)

	avg, err := avgFuture.Get()
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	} else {
		if *avg != 1.8 {
			t.Error("expected avg to be 1.8, got ", *avg)
		}
	}

	time.Sleep(1 * time.Second)
	if p.Status != statuses.Done {
		t.Errorf("expected status to be Done, got %s", p.Status.String())
	}
}

func TestFromSlice_Map_Async_AvgComplexType_Success(t *testing.T) {
	p := pipeline.FromSlice(
		[]string{"1", "2", "-3", "4", "5"},
	)

	atoiStage := transform.Map(
		p.InitStage,
		func(input string) complex64 {
			i, _ := strconv.Atoi(input)
			return complex(float32(i), float32(i))
		},
	)

	avgFuture := asyncaggregate.AvgComplexType(atoiStage)

	avg, err := avgFuture.Get()
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	} else {
		if real(*avg) != 1.8 {
			t.Error("expected avg to be 1.8, got ", *avg)
		}
	}

	time.Sleep(1 * time.Second)
	if p.Status != statuses.Done {
		t.Errorf("expected status to be Done, got %s", p.Status.String())
	}
}

func TestFromSlice_Map_Async_Max_Success(t *testing.T) {
	p := pipeline.FromSlice(
		[]string{"1", "2", "-3", "4", "5"},
	)

	atoiStage := transform.Map(
		p.InitStage,
		func(input string) int {
			i, _ := strconv.Atoi(input)
			return i
		},
	)

	maxFuture := asyncaggregate.Max(atoiStage)

	m, err := maxFuture.Get()
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	} else {
		if *m != 5 {
			t.Error("expected max to be 5, got ", *m)
		}
	}

	time.Sleep(1 * time.Second)
	if p.Status != statuses.Done {
		t.Errorf("expected status to be Done, got %s", p.Status.String())
	}
}

func TestFromSlice_Map_Async_Min_Success(t *testing.T) {
	p := pipeline.FromSlice(
		[]string{"1", "2", "-3", "4", "5"},
	)

	atoiStage := transform.Map(
		p.InitStage,
		func(input string) int {
			i, _ := strconv.Atoi(input)
			return i
		},
	)

	minFuture := asyncaggregate.Min(atoiStage)

	m, err := minFuture.Get()
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	} else {
		if *m != -3 {
			t.Error("expected min to be -3, got ", *m)
		}
	}

	time.Sleep(1 * time.Second)
	if p.Status != statuses.Done {
		t.Errorf("expected status to be Done, got %s", p.Status.String())
	}
}

func TestFromSlice_Map_Async_Count_Success(t *testing.T) {
	p := pipeline.FromSlice(
		[]string{"1", "2", "-3", "4", "5"},
	)

	atoiStage := transform.Map(
		p.InitStage,
		func(input string) int {
			i, _ := strconv.Atoi(input)
			return i
		},
	)

	countFuture := asyncaggregate.Count(atoiStage)

	count, err := countFuture.Get()
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	} else {
		if *count != 5 {
			t.Error("expected count to be 5, got ", *count)
		}
	}

	time.Sleep(1 * time.Second)
	if p.Status != statuses.Done {
		t.Errorf("expected status to be Done, got %s", p.Status.String())
	}
}

func TestFromSlice_Map_Async_Sort_Success(t *testing.T) {
	expected := []int{-3, 1, 2, 4, 5}

	p := pipeline.FromSlice(
		[]string{"1", "2", "-3", "4", "5"},
	)

	atoiStage := transform.Map(
		p.InitStage,
		func(input string) int {
			i, _ := strconv.Atoi(input)
			return i
		},
	)

	sortedFuture := asyncaggregate.Sort(atoiStage)

	sorted, err := sortedFuture.Get()
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	} else {
		sortedVal := *sorted
		if len(sortedVal) != 5 {
			t.Error("expected sorted to be of len 5, got ", len(sortedVal))
		} else {
			if !utils.SlicesEqual(sortedVal, expected) {
				t.Error("expected sorted to be [-3, 1, 2, 4, 5], got ", sortedVal)
			}
		}
	}

	time.Sleep(1 * time.Second)
	if p.Status != statuses.Done {
		t.Errorf("expected status to be Done, got %s", p.Status.String())
	}
}

func TestFromSlice_Map_Async_SortDesc_Success(t *testing.T) {
	expected := []int{5, 4, 2, 1, -3}

	p := pipeline.FromSlice(
		[]string{"1", "2", "-3", "4", "5"},
	)

	atoiStage := transform.Map(
		p.InitStage,
		func(input string) int {
			i, _ := strconv.Atoi(input)
			return i
		},
	)

	sortedFuture := asyncaggregate.SortDesc(atoiStage)

	sorted, err := sortedFuture.Get()
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	} else {
		sortedVal := *sorted
		if len(sortedVal) != 5 {
			t.Error("expected sorted to be of len 5, got ", len(sortedVal))
		} else {
			if !utils.SlicesEqual(sortedVal, expected) {
				t.Error("expected sorted to be [5, 4, 2, 1, -3], got ", sortedVal)
			}
		}
	}

	time.Sleep(1 * time.Second)
	if p.Status != statuses.Done {
		t.Errorf("expected status to be Done, got %s", p.Status.String())
	}
}

func TestFromSlice_Map_Async_GroupBy_Success(t *testing.T) {
	expected := map[string][]int{
		"even": {2, 4},
		"odd":  {1, -3, 5},
	}

	p := pipeline.FromSlice(
		[]string{"1", "2", "-3", "4", "5"},
	)

	atoiStage := transform.Map(
		p.InitStage,
		func(input string) int {
			i, _ := strconv.Atoi(input)
			return i
		},
	)

	groupedFuture := asyncaggregate.GroupBy(atoiStage, func(input int) string {
		if input%2 == 0 {
			return "even"
		} else {
			return "odd"
		}
	})

	grouped, err := groupedFuture.Get()
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	} else {
		groupedVal := *grouped
		if len(groupedVal) != 2 {
			t.Error("expected grouped to be of len 2, got ", len(groupedVal))
		} else {
			if !utils.MultiMapsEqual(groupedVal, expected) {
				t.Errorf("expected grouped to be %v, got %v", expected, groupedVal)
			}
		}
	}

	time.Sleep(1 * time.Second)
	if p.Status != statuses.Done {
		t.Errorf("expected status to be Done, got %s", p.Status.String())
	}
}

func TestFromSlice_Map_Async_Reduce_Success(t *testing.T) {
	p := pipeline.FromSlice(
		[]string{"1", "2", "-3", "4", "5"},
	)

	atoiStage := transform.Map(
		p.InitStage,
		func(input string) int {
			i, _ := strconv.Atoi(input)
			return i
		},
	)

	reducedFuture := asyncaggregate.Reduce(atoiStage, func(acc int, input int) int {
		return acc + input
	})

	reduced, err := reducedFuture.Get()
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	} else if *reduced != 9 {
		t.Errorf("expected reduced to be 9, got %d", *reduced)
	}

	time.Sleep(1 * time.Second)
	if p.Status != statuses.Done {
		t.Errorf("expected status to be Done, got %s", p.Status.String())
	}
}

func TestFromSlice_Map_Async_AsSlice_Success(t *testing.T) {
	expected := []int{1, 2, -3, 4, 5}

	p := pipeline.FromSlice(
		[]string{"1", "2", "-3", "4", "5"},
	)

	atoiStage := transform.Map(
		p.InitStage,
		func(input string) int {
			i, _ := strconv.Atoi(input)
			return i
		},
	)

	sFuture := asyncaggregate.AsSlice(atoiStage)

	s, err := sFuture.Get()
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	} else {
		sVal := *s
		if len(sVal) != 5 {
			t.Error("expected slice to be of len 5, got ", len(sVal))
		} else {
			if !utils.SlicesEqualIgnoreOrder(sVal, expected) {
				t.Error("expected slice to be [1, 2, -3, 4, 5], got ", sVal)
			}
		}
	}

	time.Sleep(1 * time.Second)
	if p.Status != statuses.Done {
		t.Errorf("expected status to be Done, got %s", p.Status.String())
	}
}

func TestFromSlice_Map_Async_AsMap_Success(t *testing.T) {
	expected := map[string]int{
		"1":  1,
		"2":  2,
		"-3": -3,
		"4":  4,
		"5":  5,
	}

	p := pipeline.FromSlice(
		[]string{"1", "2", "-3", "4", "5"},
	)

	mFuture := asyncaggregate.AsMap(
		p.InitStage,
		func(input string) types.Tuple[string, int] {
			i, _ := strconv.Atoi(input)
			return types.Tuple[string, int]{
				First:  input,
				Second: i,
			}
		})

	m, err := mFuture.Get()
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	} else {
		mVal := *m
		if len(mVal) != 5 {
			t.Error("expected map to be of len 5, got ", len(mVal))
		} else {
			if !utils.MapsEqual(mVal, expected) {
				t.Errorf("expected map to be %v, got %v", expected, mVal)
			}
		}
	}

	time.Sleep(1 * time.Second)
	if p.Status != statuses.Done {
		t.Errorf("expected status to be Done, got %s", p.Status.String())
	}
}

func TestFromSlice_Map_Async_AsMultiMap_Success(t *testing.T) {
	expected := map[string][]int{
		"even": {2, 4},
		"odd":  {1, -3, 5},
	}

	p := pipeline.FromSlice(
		[]string{"1", "2", "-3", "4", "5"},
	)

	atoiStage := transform.Map(
		p.InitStage,
		func(input string) int {
			i, _ := strconv.Atoi(input)
			return i
		},
	)

	mFuture := asyncaggregate.AsMultiMap(atoiStage, func(input int) types.Tuple[string, int] {
		if input%2 == 0 {
			return types.Tuple[string, int]{
				First:  "even",
				Second: input,
			}
		} else {
			return types.Tuple[string, int]{
				First:  "odd",
				Second: input,
			}
		}
	})

	m, err := mFuture.Get()
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	} else {
		mVal := *m
		if len(mVal) != 2 {
			t.Error("expected map to be of len 2, got ", len(mVal))
		} else {
			if !utils.MultiMapsEqual(mVal, expected) {
				t.Errorf("expected map to be %v, got %v", expected, mVal)
			}
		}
	}

	time.Sleep(1 * time.Second)
	if p.Status != statuses.Done {
		t.Errorf("expected status to be Done, got %s", p.Status.String())
	}
}

func TestFromSlice_Map_Async_ForEach_Success(t *testing.T) {
	p := pipeline.FromSlice(
		[]string{"1", "2", "-3", "4", "5"},
	)

	atoiStage := transform.Map(
		p.InitStage,
		func(input string) int {
			i, _ := strconv.Atoi(input)
			return i
		},
	)

	voidF := asyncaggregate.ForEach(atoiStage, func(i int) {
		fmt.Println(i)
	})

	_, err := voidF.Get()
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}

	time.Sleep(1 * time.Second)
	if p.Status != statuses.Done {
		t.Errorf("expected status to be Done, got %s", p.Status.String())
	}
}

func TestFromSlice_Map_Async_Distinct_Success(t *testing.T) {
	expected := []int{1, 2, -3, 4, 5}

	p := pipeline.FromSlice(
		[]string{"1", "2", "-3", "4", "5", "1", "1", "1", "5", "-3"},
	)

	atoiStage := transform.Map(
		p.InitStage,
		func(input string) int {
			i, _ := strconv.Atoi(input)
			return i
		},
	)

	sFuture := asyncaggregate.Distinct(atoiStage)

	s, err := sFuture.Get()
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	} else {
		sVal := *s
		if len(sVal) != 5 {
			t.Error("expected slice to be of len 5, got ", len(sVal))
		} else {
			if !utils.SlicesEqualIgnoreOrder(sVal, expected) {
				t.Error("expected slice to be [1, 2, -3, 4, 5], got ", sVal)
			}
		}
	}

	time.Sleep(1 * time.Second)
	if p.Status != statuses.Done {
		t.Errorf("expected status to be Done, got %s", p.Status.String())
	}
}

func TestFromSlice_Map_Async_DistinctCount_Success(t *testing.T) {
	p := pipeline.FromSlice(
		[]string{"1", "2", "-3", "4", "5", "1", "1", "1", "5", "-3"},
	)

	atoiStage := transform.Map(
		p.InitStage,
		func(input string) int {
			i, _ := strconv.Atoi(input)
			return i
		},
	)

	uniqueCountFuture := asyncaggregate.DistinctCount(atoiStage)

	uniqueCount, err := uniqueCountFuture.Get()
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	} else if *uniqueCount != 5 {
		t.Errorf("expected uniqueCount to be 5, got %d", *uniqueCount)
	}

	time.Sleep(1 * time.Second)
	if p.Status != statuses.Done {
		t.Errorf("expected status to be Done, got %s", p.Status.String())
	}
}
