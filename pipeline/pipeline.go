package pipeline

import (
	"context"
	"github.com/n0rdy/pippin/configs"
	"github.com/n0rdy/pippin/logging"
	"github.com/n0rdy/pippin/ratelimiter"
	"github.com/n0rdy/pippin/stages"
	"github.com/n0rdy/pippin/types"
	"github.com/n0rdy/pippin/types/statuses"
	"github.com/n0rdy/pippin/utils"
	"strconv"
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
	logger        logging.Logger
}

type parsedConfigs struct {
	starter             chan struct{}
	pipelineRateLimiter *ratelimiter.RateLimiter
	stageRateLimiter    *ratelimiter.RateLimiter
	timeout             time.Duration
	logger              logging.Logger
	initStageConfig     *parsedInitStageConfigs
}

type parsedInitStageConfigs struct {
	timeout time.Duration
	logger  logging.Logger
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
			p.logger.Info("Pipeline: finished with status " + status.String())
			close(p.statusChan)
			utils.StopSafely(p.timeoutTimer)
			p.logger.Close()
		}
	}
}

// FromSlice creates a pipeline from a slice.
// ctx param is used to interrupt the execution if the pipeline is interrupted.
func FromSlice[T any](s []T, confs ...configs.PipelineConfig) *Pipeline[T] {
	return from(func(ch chan<- T, ctx context.Context) {
		for _, e := range s {
			if ctx.Err() != nil {
				return
			}
			ch <- e
		}
	}, confs...)
}

// FromMap creates a pipeline from a map.
// ctx param is used to interrupt the execution if the pipeline is interrupted.
func FromMap[K comparable, V any](m map[K]V, confs ...configs.PipelineConfig) *Pipeline[types.Tuple[K, V]] {
	return from(func(ch chan<- types.Tuple[K, V], ctx context.Context) {
		for k, v := range m {
			if ctx.Err() != nil {
				return
			}
			ch <- types.Tuple[K, V]{First: k, Second: v}
		}
	}, confs...)
}

// FromChannel creates a pipeline from a channel.
// The elements from the channel will be sent to the pipeline exactly once.
// The function keeps reading from the channel until it is closed.
// Since it's an external channel, make sure to close it once it is not needed anymore, as otherwise the pipeline won't be finished.
//
// ctx param is used to interrupt the execution if the pipeline is interrupted.
func FromChannel[T any](fromCh <-chan T, confs ...configs.PipelineConfig) *Pipeline[T] {
	return from(func(ch chan<- T, ctx context.Context) {
		running := true
		for running {
			select {
			case e, ok := <-fromCh:
				if ok {
					ch <- e
				} else {
					running = false
				}
			case <-ctx.Done():
				return
			}
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
// The [configs.PipelineConfig.Timeout] config can be used to set the timeout for the pipeline.
//
// Use [configs.PipelineConfig.Logger] to set the logger for the pipeline.
//
// The [configs.PipelineConfig.InitStageConfig] config can be used to configure the initial stage.
// See [configs.StageConfig] for more details.
func from[T any](pipelineInitFunc func(chan<- T, context.Context), confs ...configs.PipelineConfig) *Pipeline[T] {
	initChan := make(chan T)
	pipelineStatusChan := make(chan statuses.Status)
	ctx, ctxCancelFunc := context.WithCancel(context.Background())

	pc := parseConfigs(confs...)

	starter := pc.starter
	var stageStarter chan struct{}
	if starter != nil {
		stageStarter = make(chan struct{})
	}
	pipelineLogger := pc.logger

	pipelineLogger.Debug("Pipeline: initiating...")

	var status statuses.Status
	if starter == nil {
		status = statuses.Running
	} else {
		status = statuses.Pending
	}

	initStage := stages.NewInitStage(
		initChan, pc.pipelineRateLimiter, pc.stageRateLimiter, stageStarter,
		ctx, ctxCancelFunc, pipelineStatusChan, pipelineLogger,
	)
	p := &Pipeline[T]{
		InitStage:     initStage,
		Status:        status,
		rateLimiter:   pc.pipelineRateLimiter,
		starter:       starter,
		ctx:           ctx,
		ctxCancelFunc: ctxCancelFunc,
		statusChan:    pipelineStatusChan,
		logger:        pipelineLogger,
	}

	go p.listenToStatusUpdates()

	localLogger, pipelineSpecific := localLogger(pipelineLogger, confs...)
	if pipelineSpecific {
		defer localLogger.Close()
	}
	localTimeout := localTimeout(confs...)

	var stageIdAsString string
	customStageId := customStageId(confs...)
	if customStageId != 0 {
		stageIdAsString = "stage " + strconv.FormatInt(customStageId, 10) + ": "
	} else {
		stageIdAsString = "stage " + strconv.FormatInt(stages.InitStageId, 10) + ": "
	}

	go func() {
		defer close(initChan)

		if starter != nil {
			pipelineLogger.Debug("Pipeline: waiting for the start signal...")
			localLogger.Debug(stageIdAsString + "waiting for the start signal...")

			select {
			case _, ok := <-starter:
				if ok {
					pipelineLogger.Debug("Pipeline: start signal received")
					localLogger.Debug(stageIdAsString + "start signal received")
					stageStarter <- struct{}{}
					close(starter)
				}
			case <-ctx.Done():
				pipelineLogger.Debug("Pipeline: interrupted before the start signal")
				localLogger.Debug(stageIdAsString + "context done signal received before the start signal")
				// if the pipeline is interrupted before it is started, then return
				close(starter)
				return
			}
		}

		pipelineLogger.Info("Pipeline: started")
		localLogger.Info(stageIdAsString + "started")

		// start pipeline timeout if configured
		go func() {
			if pc.timeout > 0 {
				p.timeoutTimer = time.AfterFunc(pc.timeout, func() {
					pipelineLogger.Info("Pipeline: timeout reached for pipeline - interrupting the pipeline")
					ctxCancelFunc()
					pipelineStatusChan <- statuses.TimedOut
				})
			}
		}()

		// start stage timeout if configured
		var stageTimeoutTimer *time.Timer
		go func() {
			if localTimeout > 0 {
				stageTimeoutTimer = time.AfterFunc(localTimeout, func() {
					localLogger.Info(stageIdAsString + "timeout reached for stage - interrupting the pipeline" + stageIdAsString + " - interrupting the pipeline")
					ctxCancelFunc()
					pipelineStatusChan <- statuses.TimedOut
				})
			}
		}()

		initFuncCtx, initFuncCancelFunc := context.WithCancel(initStage.Context())
		defer initFuncCancelFunc()

		pipelineInitFunc(initChan, initFuncCtx)

		utils.StopSafely(stageTimeoutTimer)
		localLogger.Info(stageIdAsString + "finished")
	}()

	return p
}

func parseConfigs(confs ...configs.PipelineConfig) *parsedConfigs {
	var starter chan struct{}
	var pipelineRateLimiter *ratelimiter.RateLimiter
	var stageRateLimiter *ratelimiter.RateLimiter
	var timeout time.Duration
	var logger logging.Logger
	var parsedInitStageConf *parsedInitStageConfigs

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
		if conf.Timeout > 0 {
			timeout = conf.Timeout
		}
		if conf.Logger != nil {
			logger = conf.Logger
		}

		initStageConf := conf.InitStageConfig
		if initStageConf != nil {
			parsedInitStageConf = &parsedInitStageConfigs{}

			if initStageConf.Timeout > 0 {
				parsedInitStageConf.timeout = initStageConf.Timeout
			}
			if initStageConf.Logger != nil {
				parsedInitStageConf.logger = initStageConf.Logger
			}
		}
	}

	if logger == nil {
		logger = logging.NewNoOpsLogger()
	}

	return &parsedConfigs{
		starter:             starter,
		pipelineRateLimiter: pipelineRateLimiter,
		stageRateLimiter:    stageRateLimiter,
		timeout:             timeout,
		logger:              logger,
		initStageConfig:     parsedInitStageConf,
	}
}

func localLogger(pipelineLogger logging.Logger, confs ...configs.PipelineConfig) (logging.Logger, bool) {
	if len(confs) == 0 {
		return pipelineLogger, false
	}

	conf := confs[0].InitStageConfig
	if conf != nil && conf.Logger != nil {
		// stage configs overrides pipeline configs for logger
		return conf.Logger, true
	}
	return pipelineLogger, false
}

func localTimeout(confs ...configs.PipelineConfig) time.Duration {
	if len(confs) == 0 {
		return 0
	}

	conf := confs[0].InitStageConfig
	if conf != nil {
		// stage configs overrides pipeline configs for timeout
		return conf.Timeout
	}
	return 0
}

func customStageId(confs ...configs.PipelineConfig) int64 {
	if len(confs) == 0 {
		return 0
	}

	conf := confs[0].InitStageConfig
	if conf != nil {
		return conf.CustomId
	}
	return 0
}
