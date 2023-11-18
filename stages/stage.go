package stages

import (
	"context"
	"github.com/n0rdy/pippin/ratelimiter"
	"github.com/n0rdy/pippin/types/statuses"
)

const (
	initStageId = 1
)

// Stage is a struct that represents a stage in a pipeline.
// It is created either by a pipeline (the initial stage only) via the [NewInitStage] function, or by another stage via the [FromStage] function.
//
// The stage exposes:
//
// the [Stage.InterruptPipeline] function, which interrupts the pipeline.
// It is intended for internal use only.
// If you need to stop the pipeline, don't use this function, call the [pipeline.Pipeline.Interrupt] function instead;
//
// the [Stage.SetPipelineStatus] function, which sets the status of the pipeline;
// This function is for the internal use only. The new status is propagated to the pipeline via the channel, that's why only the eventual consistency is guaranteed;
//
// the [Stage.Context] function, which returns the context of the stage;
//
// the [Stage.Id] field, which is the id of the stage.
// This field is not used internally as of today, but it might be a part of a future features.
// It might be a good idea to use this field to identify the stage in the logs if needed;
//
// the [Stage.Chan] field, which is the read-only channel of the stage.
// The stage reads from this channel and passes the data to the next stage.
// The channel is created and closed by the previous stage or the pipeline (for the initial stage only);
//
// the [Stage.Wg] field, which is the wait group of the stage.
// The stage passes it to the next stage and waits for it to finish before exiting and closing the channel of the next stage;
//
// the [Stage.PipelineRateLimiter] field, which is the rate limiter of the pipeline.
// If present, it is used to limit the max number of goroutines that can be created by the pipeline.
// The stage passes it to the next stage;
//
// the [Stage.StageRateLimiter] field, which is the rate limiter of the stage.
// If present, it is used to limit the max number of goroutines that can be created by the stage.
// The stage passes it to the next stage, however, the next stage can create its own rate limiter and use it instead
// if the [configs.StageConfig.MaxGoroutines] option is provided for the next stage;
//
// the [Stage.Starter] field, which is the channel that is used to start the stage.
// It is used for the delayed manual pipeline start feature to start each stage.
type Stage[T any] struct {
	Id                    int64
	Chan                  <-chan T
	PipelineRateLimiter   *ratelimiter.RateLimiter
	StageRateLimiter      *ratelimiter.RateLimiter
	Starter               chan struct{}
	stageCtx              context.Context
	stageCtxCancelFunc    context.CancelFunc
	pipelineCtxCancelFunc context.CancelFunc
	pipelineStatusChan    chan<- statuses.Status
}

// NewInitStage is a function that creates a new stage based on the provided parameters.
func NewInitStage[T any](ch <-chan T, pipelineRateLimiter *ratelimiter.RateLimiter, stageRateLimiter *ratelimiter.RateLimiter, starter chan struct{}, pipelineCtx context.Context, pipelineCtxCancelFunc context.CancelFunc, pipelineStatusChan chan<- statuses.Status) Stage[T] {
	stageCtx, stageCtxCancelFunc := context.WithCancel(pipelineCtx)

	return Stage[T]{
		Id:                    initStageId,
		Chan:                  ch,
		PipelineRateLimiter:   pipelineRateLimiter,
		StageRateLimiter:      stageRateLimiter,
		Starter:               starter,
		stageCtx:              stageCtx,
		stageCtxCancelFunc:    stageCtxCancelFunc,
		pipelineCtxCancelFunc: pipelineCtxCancelFunc,
		pipelineStatusChan:    pipelineStatusChan,
	}
}

// FromStage is a function that creates a new stage based on the provided parameters and the previous stage.
// The following arguments are reused from the previous stage:
// [Stage.PipelineRateLimiter]
// [Stage.pipelineCtxCancelFunc]
// [Stage.pipelineStatusChan]
//
// The new state context is created based on the previous stage context.
func FromStage[In, Out any](stage Stage[In], ch <-chan Out, starter chan struct{}) Stage[Out] {
	stageCtx, stageCtxCancelFunc := context.WithCancel(stage.stageCtx)

	return Stage[Out]{
		Id:                    stage.Id + 1,
		Chan:                  ch,
		PipelineRateLimiter:   stage.PipelineRateLimiter,
		StageRateLimiter:      ratelimiter.Copy(stage.StageRateLimiter),
		Starter:               starter,
		stageCtx:              stageCtx,
		stageCtxCancelFunc:    stageCtxCancelFunc,
		pipelineCtxCancelFunc: stage.pipelineCtxCancelFunc,
		pipelineStatusChan:    stage.pipelineStatusChan,
	}
}

// InterruptPipeline is a function that interrupts the pipeline.
// It is intended for internal use only.
// If you need to interrupt the pipeline, call the [pipeline.Pipeline.Interrupt] function instead.
func (s *Stage[T]) InterruptPipeline() {
	s.pipelineCtxCancelFunc()
}

// SetPipelineStatus is a function that sets the status of the pipeline.
// This function is for the internal use only.
// The new status is propagated to the pipeline via the channel, that's why only the eventual consistency is guaranteed.
func (s *Stage[T]) SetPipelineStatus(status statuses.Status) {
	s.pipelineStatusChan <- status
}

// Context is a function that returns the context of the stage.
func (s *Stage[T]) Context() context.Context {
	return s.stageCtx
}
