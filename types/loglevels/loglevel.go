package loglevels

const (
	TRACE = iota
	DEBUG
	INFO
	WARN
	ERROR

	TracePrefix = " [TRACE] "
	DebugPrefix = " [DEBUG] "
	InfoPrefix  = " [INFO] "
	WarnPrefix  = " [WARN] "
	ErrorPrefix = " [ERROR] "
)

// LogLevel is a type that represents a log level.
// The hierarchy of log levels is the following: TRACE < DEBUG < INFO < WARN < ERROR.
type LogLevel int

func (ll LogLevel) String() string {
	switch ll {
	case TRACE:
		return "TRACE"
	case DEBUG:
		return "DEBUG"
	case INFO:
		return "INFO"
	case WARN:
		return "WARN"
	case ERROR:
		return "ERROR"
	default:
		return "UNKNOWN"
	}
}
