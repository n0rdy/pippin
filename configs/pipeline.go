package configs

// PipelineConfig is a struct that contains the configuration for a pipeline
//
// [PipelineConfig.ManualStart] is a boolean that indicates whether the pipeline should be started manually.
// If it is passed as true, the pipeline will not start automatically on creation, and it's up to the user to start it by calling the [pipeline.Pipeline.Start] method.
//
// [PipelineConfig.MaxGoroutinesTotal] is an integer that indicates the maximum number of goroutines that can be spawned by the pipeline.
// If it is passed as 0 or less, then there is no limit.
// Please, note that the real number of goroutines is always greater than the defined size, as:
// - there are service goroutines that are not limited by the rate limiter
// - even if the pipeline rate limiter is full, the program will spawn a new goroutine if there is no workers for the current stage
//
// [PipelineConfig.MaxGoroutinesPerStage] is an integer that indicates the maximum number of goroutines that can be spawned by each stage.
// If it is passed as 0 or less, then there is no limit.
// It is possible to change the limit for each stage individually - see [StageConfig.MaxGoroutines].
//
// [PipelineConfig.TimeoutInMillis] is an integer that indicates the timeout (in milliseconds) for the pipeline.
// If it is passed as 0 or less, then there is no timeout.
type PipelineConfig struct {
	ManualStart           bool
	MaxGoroutinesTotal    int
	MaxGoroutinesPerStage int
	TimeoutInMillis       int64
}
