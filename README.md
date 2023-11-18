# Pippin

Pippin is a simple, lightweight, and (hopefully) easy to use Go library for creating and managing data pipelines.

The library heavily relies on goroutines and channels, but this complexity is hidden from the user behind a simple API.
Basically, this is the main purpose why I implemented this library in the first place.

It has no external dependencies except for the two Go standard library experimental packages:
- `golang.org/x/exp`
- `golang.org/x/sync`

and one external dependency for testing goroutine leaks:
- `go.uber.org/goleak`

Please, note that the library is still in the early development stage, so the API might change in the future.
There still might be some bugs, so please, feel free to report them.

### But we already have [insert library here]!

We've had one, yes. But what about second ~~breakfast~~ library?

## Table of contents
* [Usage](#usage)
* [Documentation](#documentation)
* [Concepts](#concepts)
    * [Pipeline](#pipeline)
        * [Creation](#creation)
        * [Configuration](#configuration)
        * [Manual start](#manual-start)
        * [Interrupting](#interrupting)
    * [Stage](#stage)
        * [Transformation](#transformation)
        * [Aggregation](#aggregation)
        * [Future](#future)
        * [Configuration](#configuration-1)


## Installation

```bash
go get github.com/n0rdy/pippin
```

## Usage

### Simple example

```go
// create a new pipeline from a slice of integers:
p := pipeline.FromSlice[int]([]int{1, 2, 3, 4, 5})

// filters out all even numbers:
filteringStage := transform.Filter[int](p.InitStage, func(i int) bool {
    return i % 2 == 0
})

// multiplies each number by 2:
mappingStage := transform.Map[int, int](filteringStage, func(i int) int {
    return i * 2
})

// sums all numbers:
res, err := aggregate.Sum[int](mappingStage)
if err != nil {
    fmt.Println(err)
} else {
    fmt.Println(*res)	
}

// the output is:
// 12
```

### More detailed example

```go
// create a new pipeline from a slice of integers:
p := pipeline.FromSlice(
	[]string{"1", "a", "2", "-3", "4", "5", "b"},
)
// result:
// "1", "a", "2", "-3", "4", "5", "b"

atoiStage := transform.MapWithError(
	p.InitStage,
	func(input string) (int, error) {
		return strconv.Atoi(input)
	},
	func(err error) {
		fmt.Println(err)
	},
)
// result:
// 1, 2, -3, 4, 5
// printed to the console: 
// strconv.Atoi: parsing "a": invalid syntax 
// strconv.Atoi: parsing "b": invalid syntax

oddNumsStage := transform.Filter(atoiStage, func(input int) bool {
	return input%2 != 0
})
// result:
// 1, -3, 5

multipliedByTwoStage := transform.Map(oddNumsStage, func(input int) int {
	return input * 2
})
// result:
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
// result:
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
// result:
// [3], [43], [11], [21], [31], [41], [51], [61], [71], [81], [91]
// printed to the console:
// zero
// zero

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
// result:
// [0], [43], [0], [0], [0], [0], [51], [61], [71], [81], [91]

flattenedStage := transform.FlatMap(greaterThan42Stage, func(input int) int {
	return input
})
// result:
// [0, 43, 0, 0, 0, 0, 51, 61, 71, 81, 91]

futureSum := asyncaggregate.Sum(flattenedStage)
// result:
// 398

result, err := futureSum.GetWithTimeout(time.Duration(10)*time.Second)
if err != nil {
	fmt.Println(err)
} else {
	fmt.Println(*result)
}
// printed to the console:
// 398
```

## Documentation

Find the full documentation [here](https://pkg.go.dev/github.com/n0rdy/pippin).

## Concepts

The main concepts of Pippin are:
- pipeline
- stage

### Pipeline

Pipeline is a sequence of stages.
It is the first and the key object in the library. Its API provides a possibility to interrupt the entire pipeline.

The user can see the pipeline current status by accessing the `Status` field. 
Please, note that the status is updated asynchronously, so it may not be up-to-date right away after the change - eventual consistency.

#### Creation

It is created from a variety of sources, such as:
- slice
- map
- channel

To create a pipeline from a slice:
```go
p := pipeline.FromSlice[int]([]int{1, 2, 3, 4, 5})
```

To create a pipeline from a map:
```go
m := map[string]int{
    "one": 1,
    "two": 2,
    "three": 3,
}
p := pipeline.FromMap[string, int](m)
```

To create a pipeline from a channel:
```go
ch := make(chan int)
p := pipeline.FromChannel[int](ch)
```

#### Configuration

It is possible to configure the pipeline by using the `configs.PipelineConfig` struct, which contains the following config options:
- `ManualStart` - is a boolean that indicates whether the pipeline should be started manually. 
If it is passed as `true`, the pipeline will not start automatically on creation, and it's up to the user to start it by calling the `pipeline.Pipeline.Start` method.
- `MaxGoroutinesTotal` - is an integer that indicates the maximum number of goroutines that can be spawned by the pipeline. 
If it is passed as `0` or less, then there is no limit. 
Please, note that the real number of goroutines is always greater than the defined size, as there are service goroutines that are not limited by the rate limiter, 
and even if the pipeline rate limiter is full, the program will spawn a new goroutine if there is no workers for the current stage.
- `MaxGoroutinesPerStage` - is an integer that indicates the maximum number of goroutines that can be spawned by each stage. 
If it is passed as `0` or less, then there is no limit. 
It is possible to change the limit for each stage individually - see `configs.StageConfig.MaxGoroutines`.
- `TimeoutInMillis` - is an integer that indicates the timeout in milliseconds for the entire pipeline. 
If it is passed as `0` or less, then there is no timeout.

If you pipeline performs any network calls within its transformation/aggregation logic, I'd suggest configuring the maximum number of goroutines to prevent the possible DDoS attack on the target server or reaching the maximum number of open files on the client machine.

To create a pipeline with a custom configuration:
```go
p := pipeline.FromSlice[int]([]int{1, 2, 3, 4, 5}, configs.PipelineConfig{
    ManualStart: true,
    MaxGoroutinesTotal: 100,
    MaxGoroutinesPerStage: 10,
    TimeoutInMillis: 1000,
})
```

Please, note that even though it is technically possible to pass more than one configuration option, only the first one will be used.

#### Manual start

If the pipeline is configured to be started manually via the `ManualStart` option, it won't start automatically on creation. 
In order to start it, do:
```go
p := pipeline.FromSlice[int]([]int{1, 2, 3, 4, 5}, configs.PipelineConfig{
    ManualStart: true,
})

// some code here

p.Start()
```

#### Interrupting

It is possible to interrupt the pipeline by:
```go
p := pipeline.FromSlice[int]([]int{1, 2, 3, 4, 5})

// some code here

p.Interrupt()
```

This method gracefully tries to interrupt the pipeline. There is no guarantee that the pipeline will be interrupted immediately.

### Stage

Stage is a single step in the pipeline. It is created either by a pipeline (the initial stage only), or by another stage. It contains no values within itself.

The high-level picture is the following: first the user needs to create a pipeline object and then perform some actions on it that will lead to the creation of stages.

There are two types of actions the user can perform:
- transformation
- aggregation

#### Transformation

Transformation is an intermediate action that transforms the data in the pipeline. It is performed by the `transform` package.

If you are coming from the JVM world, you can think of it as `Stream` transformations in Java 8+/Scala.

To create a transformation, provide a stage, a transformation function, and an optional configuration.
As a result, a new stage will be created with the type of the transformation function's output.

Pippin provides the following transformation functions:
- `Map`
- `MapWithError`
- `MapWithErrorMapper`
- `FlatMap`
- `FlatMapWithError`
- `FlatMapWithErrorMapper`
- `Filter`

`Map`, `FlatMap` and `Filter` are the same as in Java/Scala/Kotlin. `WithError` and `WithErrorMapper` functions are the same as their counterparts, but they also provide a possibility to handle errors:
- `WithError` handles errors by performing a function with a side effect on each error
- `WithErrorMapper` handles errors by mapping them to the output type

To simplify this, use `WithError` if you'd like to, for example, log the error and continue the pipeline by ignoring the input element that caused the error.
```go
p := pipeline.FromSlice[string]([]string{"1", "2", "a", "3"})

// converts each string to an integer
// when an error happens, it will be logged to the console
atoiStage := transform.MapWithError(
    p.InitStage,
    func(s string) (int, error) {
        return strconv.Atoi(s)
    },
    func(err error) {
        fmt.Println("error happened", err)
    },
)
```
Use `WithErrorMapper` if you'd like to provide a default output value for the error.
```go
p := pipeline.FromSlice[string]([]string{"1", "2", "a", "3"})

// converts each string to an integer
// when an error happens, a default value of 42 will be used
atoiStage := transform.MapWithErrorMapper(
    p.InitStage,
    func(s string) (int, error) {
    	return strconv.Atoi(s)
    },
    func(err error) int {
    	return 42
    },
)
```

#### Aggregation

As mentioned above, the transformations are the intermediate actions. It means that it is possible to chain them together in one by one fashion in order to create a pipeline. However, transformation doesn't return any result, only a stage. 
In order to get the result, the user needs to perform an aggregation, which is the last step in the pipeline.

Pippin provides the following aggregation functions:
- `Sum`
- `SumComplexType`
- `Avg`
- `AvgComplexType`
- `Max`
- `Min`
- `Count`
- `Sort`
- `SortDesc`
- `GroupBy`
- `Reduce`
- `AsSlice`
- `AsMap`
- `AsMultiMap`
- `ForEach`
- `Distinc`
- `DistinctCount`

Hopefully, the names are self-explanatory. The only thing to note is that `Sum` and `Avg` functions are for numeric types only, while `SumComplexType` and `AvgComplexType` are for complex types such as `complex64` and `complex128`.

To create an aggregation, provide a stage and an optional configuration.
```go
p := pipeline.FromSlice[int]([]int{1, 2, 3, 4, 5})

// multiplies each number by 2:
mappingStage := transform.Map[int, int](filteringStage, func(i int) int {
    return i * 2
})

// sums all numbers:
res, err := aggregate.Sum[int](mappingStage)
```

Pippin implements two types of aggregations: 
- synchronous - `aggregation` package
- asynchronous - `asyncaggregation` package

The name of the functions and the arguments are the same for both packages, but the return types are different:
- synchronous returns the pointer to the result and the error
- asynchronous returns a `types.Future` object that contains either the pointer to the result or the error within

The key difference between the two is the fact that the synchronous aggregation blocks the current goroutine until the result is ready, while the asynchronous one doesn't.
That's why async one returns `Future` object.

If the pipeline is interrupted before the result is ready, the synchronous aggregation will return an error, while the asynchronous one will return a `Future` object with an error within.

Please, note that it is not possible to set up the delayed manual start for the pipeline if the synchronous aggregation is used - the code will panic.

#### Future

`Future` object is the concept similar to Java/Scala Future-s or JavaScript Promises. 
It is an object that represents the result of an asynchronous computation that is going to be available in the future. 
This is the way to avoid blocking the execution and a way to early return from a function.

There are two ways to do that:
- by calling `Get()` method. This method will block until the value is available. It returns either the pointer to the value or an error.
In Pippin the error means that the pipeline was interrupted before it could complete that's why the value is not available.
- by calling `GetWithTimeout(timeout time.Duration)` method. This method will block until the value is available or the timeout is reached.

The recommended way to obtain the value is by calling `GetWithTimeout` method, as otherwise the execution might be blocked forever.

It is possible to manually check whether the future is done or not by calling `IsDone` method.
This method return a boolean value indicating whether the future is done or not. It is not blocking.

Please, note that since it's the async operation, the value might not be available immediately.

#### Configuration

Both transformation and aggregation functions accept an optional configuration argument similar to the pipeline configuration.
It is represented by the `configs.StageConfig` struct, which contains the following config options:
- `MaxGoroutines` - is an integer that indicates the maximum number of goroutines that can be spawned by the stage. 
If it is passed as `0` or less, then there is no limit. 
This config option can be used to change the limit for each stage that comes from the `configs.PipelineConfig.MaxGoroutinesPerStage` option (if provided).
Please, note that the real number of goroutines might be higher than the number specified here, as the library spawns additional goroutines for internal purposes.
- `TimeoutInMillis` - is an integer that indicates the timeout in milliseconds for the stage. If it is passed as `0` or less, then there is no timeout.
- `StageConfig.CustomId` - is a custom ID for the stage. If it is passed as 0, then the stage will be assigned an ID automatically. 
Auto-generated IDs are calculated as follows: 1 + the ID of the previous stage. 
The initial stage (the one that is created first) has an ID of 1. It is recommended to either rely on the auto-generated IDs or to provide a custom ID for each stage, otherwise the IDs might be messed up due to the (1 + the ID of the previous stage) logic mentioned above.

To create a transformation with a custom configuration:
```go
p := pipeline.FromSlice[int]([]int{1, 2, 3, 4, 5})

// multiplies each number by 2:
mappingStage := transform.Map[int, int](filteringStage, func(i int) int {
    return i * 2
}, configs.StageConfig{
    MaxGoroutines: 10,
    TimeoutInMillis: 1000,
    CustomId: 1,
})
```

Please, note that even though it is technically possible to pass more than one configuration option, only the first one will be used.


Have fun =)
