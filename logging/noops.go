package logging

// NoOpsLogger is a logger that does nothing.
// This is a default logger for the service if no logger is configured.
type NoOpsLogger struct{}

func NewNoOpsLogger() Logger {
	return &NoOpsLogger{}
}

func (nol *NoOpsLogger) Trace(message string) {}

func (nol *NoOpsLogger) Debug(message string) {}

func (nol *NoOpsLogger) Info(message string) {}

func (nol *NoOpsLogger) Warn(message string, errs ...error) {}

func (nol *NoOpsLogger) Error(message string, errs ...error) {}

func (nol *NoOpsLogger) Close() error {
	return nil
}
