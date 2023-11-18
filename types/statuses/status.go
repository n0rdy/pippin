package statuses

const (
	Pending = iota
	Running
	Done
	Interrupted
	TimedOut
)

// Status represents the status of a pipeline.
// The sequence of statuses is: pending -> running -> done / interrupted / timedOut
//
// Pending - once the delayed manual start option is chosen via the [configs.PipelineConfig.ManualStart] until the pipeline is started.
// Running - once the pipeline is started.
// Done - once the pipeline is finished successfully.
// Interrupted - once the pipeline is interrupted by the user.
// TimedOut - once the pipeline is finished with a timeout.
type Status int

func (s Status) String() string {
	switch s {
	case Pending:
		return "pending"
	case Running:
		return "running"
	case Done:
		return "done"
	case Interrupted:
		return "interrupted"
	case TimedOut:
		return "timedOut"
	default:
		return "unknown"
	}
}
