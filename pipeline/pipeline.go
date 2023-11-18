package pipeline

import (
	"context"
	"pippin/configs"
	"pippin/ratelimiter"
	"pippin/stages"
	"pippin/types"
	"pippin/types/statuses"
	"pippin/utils"
	"time"
)

// Pipeline is a data processing pipeline.
// It is created with the [FromSlice], [FromMap] or [FromChannel] functions.
// The pipeline consists of stages, which are created with the functions from the [stages] package.
//
// The pipeline object contains only the initial stage, and there is no way to navigate the stages explicitly within the [Pipeline] object.
//
// The pipeline exposes:
// the [Pipeline.Start] method, which starts the pipeline if it was created with the delayed manual start;
// the [Pipeline.Interrupt] method, which gracefully tries to interrupt the pipeline;
// the [Pipeline.Close] method, which closes the pipeline resources;
// the [Pipeline.InitStage] field, which is the initial stage of the pipeline;
// the [Pipeline.Status] field, which is the current status of the pipeline.
//
// Please, note that the status is updated asynchronously, so it may not be up-to-date right away after the change - eventual consistency.
type Pipeline[A any] struct {
	InitStage     stages.Stage[A]
	Status        statuses.Status
	rateLimiter   *ratelimiter.RateLimiter
	starter       chan struct{}
	timeoutTimer  *time.Timer
	ctx           context.Context
	ctxCancelFunc context.CancelFunc
	statusChan    chan statuses.Status
}

type parsedConfigs struct {
	starter             chan struct{}
	pipelineRateLimiter *ratelimiter.RateLimiter
	stageRateLimiter    *ratelimiter.RateLimiter
	timeout             time.Duration
}

// Start starts the pipeline if it was created with the delayed manual start.
// If the pipeline was already started, then this method does nothing and returns false.
func (p *Pipeline[A]) Start() bool {
	if p.Status == statuses.Pending {
		p.starter <- struct{}{}
		p.Status = statuses.Running
		return true
	}
	return false
}

// Interrupt gracefully tries to interrupt the pipeline.
// There is no guarantee that the pipeline will be interrupted immediately.
func (p *Pipeline[A]) Interrupt() {
	if p.Status == statuses.Interrupted || p.Status == statuses.Done || p.Status == statuses.TimedOut {
		return
	}

	p.ctxCancelFunc()
	p.statusChan <- statuses.Interrupted
}

// Close closes the pipeline resources.
func (p *Pipeline[A]) Close() error {
	ratelimiter.CloseSafely(p.rateLimiter)
	return nil
}

// listenToStatusUpdates listens to the status changes and updates the pipeline status accordingly.
func (p *Pipeline[A]) listenToStatusUpdates() {
	for status := range p.statusChan {
		p.Status = status

		switch status {
		case statuses.Done, statuses.Interrupted, statuses.TimedOut:
			close(p.statusChan)
			utils.StopSafely(p.timeoutTimer)
		}
	}
}

// FromSlice creates a pipeline from a slice.
func FromSlice[T any](s []T, confs ...configs.PipelineConfig) *Pipeline[T] {
	return from(func(ch chan<- T) {
		// TODO: not possible to interrupt the pipeline if the slice is not iterated over
		for _, e := range s {
			ch <- e
		}
	}, confs...)
}

// FromMap creates a pipeline from a map.
func FromMap[K comparable, V any](m map[K]V, confs ...configs.PipelineConfig) *Pipeline[types.Tuple[K, V]] {
	return from(func(ch chan<- types.Tuple[K, V]) {
		// TODO: not possible to interrupt the pipeline if the map is not iterated over
		for k, v := range m {
			ch <- types.Tuple[K, V]{First: k, Second: v}
		}
	}, confs...)
}

// FromChannel creates a pipeline from a channel.
// The elements from the channel will be sent to the pipeline exactly once.
// The function keeps reading from the channel until it is closed.
// Since it's an external channel, make sure to close it once it is not needed anymore, as otherwise the pipeline won't be finished.
func FromChannel[T any](fromCh <-chan T, confs ...configs.PipelineConfig) *Pipeline[T] {
	return from(func(ch chan<- T) {
		// TODO: not possible to interrupt the pipeline if the channel is not closed
		for e := range fromCh {
			ch <- e
		}
	}, confs...)

}

// from creates a pipeline from a pipeline init function.
// As a result, a [Pipeline] object is returned with the initial stage [stages.Stage] within.
// This [stages.Stage] object should be passed to the next stages like transform and/or aggregate.
//
// The [from] function accepts pipeline configs as optional parameters. Only the first config is used, the rest are ignored.
// The configs are used to configure the pipeline:
//
// The [configs.PipelineConfig.ManualStart] config can be used to delay the pipeline start until [pipeline.Pipeline.Start] is called.
//
// It is also possible to limit the number of goroutines that can be spawned:
// use [configs.PipelineConfig.MaxGoroutinesTotal] to limit the number of goroutines for the whole pipeline,
// use [configs.PipelineConfig.MaxGoroutinesPerStage] to limit the number of goroutines per stage.
// There is a possibility to change the limit for each stage individually - see [configs.StageConfig.MaxGoroutines].
// If the limit is reached, then the pipeline will wait until the number of goroutines is decreased.
//
// The [configs.PipelineConfig.TimeoutInMillis] config can be used to set the timeout for the pipeline.
func from[T any](pipelineInitFunc func(chan<- T), confs ...configs.PipelineConfig) *Pipeline[T] {
	initChan := make(chan T)
	pipelineStatusChan := make(chan statuses.Status)
	ctx, ctxCancelFunc := context.WithCancel(context.Background())

	pc := parseConfigs(confs...)

	starter := pc.starter
	var stageStarter chan struct{}
	if starter != nil {
		stageStarter = make(chan struct{})
	}

	var status statuses.Status
	if starter == nil {
		status = statuses.Running
	} else {
		status = statuses.Pending
	}

	p := &Pipeline[T]{
		InitStage: stages.NewInitStage(
			initChan, pc.pipelineRateLimiter, pc.stageRateLimiter,
			stageStarter, ctx, ctxCancelFunc, pipelineStatusChan,
		),
		Status:        status,
		rateLimiter:   pc.pipelineRateLimiter,
		starter:       starter,
		ctx:           ctx,
		ctxCancelFunc: ctxCancelFunc,
		statusChan:    pipelineStatusChan,
	}

	go p.listenToStatusUpdates()

	go func() {
		defer close(initChan)

		if starter != nil {
			select {
			case _, ok := <-starter:
				if ok {
					stageStarter <- struct{}{}
					close(starter)
				}
			case <-ctx.Done():
				// if the pipeline is interrupted before it is started, then return
				close(starter)
				return
			}
		}

		go func() {
			if pc.timeout > 0 {
				p.timeoutTimer = time.AfterFunc(pc.timeout, func() {
					ctxCancelFunc()
					pipelineStatusChan <- statuses.TimedOut
				})
			}
		}()

		pipelineInitFunc(initChan)
	}()

	return p
}

func parseConfigs(confs ...configs.PipelineConfig) *parsedConfigs {
	var starter chan struct{}
	var pipelineRateLimiter *ratelimiter.RateLimiter
	var stageRateLimiter *ratelimiter.RateLimiter
	var timeout time.Duration

	if len(confs) > 0 {
		conf := confs[0]
		if conf.ManualStart {
			starter = make(chan struct{})
		}
		if conf.MaxGoroutinesTotal > 0 {
			pipelineRateLimiter = ratelimiter.NewRateLimiter(conf.MaxGoroutinesTotal)
		}
		if conf.MaxGoroutinesPerStage > 0 {
			stageRateLimiter = ratelimiter.NewRateLimiter(conf.MaxGoroutinesPerStage)
		}
		if conf.TimeoutInMillis > 0 {
			timeout = time.Duration(conf.TimeoutInMillis) * time.Millisecond
		}
	}

	return &parsedConfigs{
		starter:             starter,
		pipelineRateLimiter: pipelineRateLimiter,
		stageRateLimiter:    stageRateLimiter,
		timeout:             timeout,
	}
}
