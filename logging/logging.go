package logging

type Logger interface {
	Trace(message string)
	Debug(message string)
	Info(message string)
	Warn(message string, errs ...error)
	Error(message string, errs ...error)
	Close() error
}
