package logging

import (
	"fmt"
	"github.com/n0rdy/pippin/types/loglevels"
	"github.com/n0rdy/pippin/utils"
)

// ConsoleLogger is a logger that prints logs to console.
// Basically, it uses fmt.Println() to print logs under the hood.
//
// ConsoleLogger accepts a log level as a parameter.
// It will print only those logs that have a level equal or higher than the specified one.
// Check [loglevels.LogLevel] for more details.
//
// The format of logs is: "time [log level] message"
// Example:
// 2006-01-02 15:04:05:000 [INFO] some cool info message
type ConsoleLogger struct {
	level loglevels.LogLevel
}

func NewConsoleLogger(level loglevels.LogLevel) Logger {
	return &ConsoleLogger{
		level: level,
	}
}

func (cl *ConsoleLogger) Trace(message string) {
	if cl.level <= loglevels.TRACE {
		fmt.Println(utils.TimeNowAsRFC3339NanoString() + loglevels.TracePrefix + message)
	}
}

func (cl *ConsoleLogger) Debug(message string) {
	if cl.level <= loglevels.DEBUG {
		fmt.Println(utils.TimeNowAsRFC3339NanoString() + loglevels.DebugPrefix + message)
	}
}

func (cl *ConsoleLogger) Info(message string) {
	if cl.level <= loglevels.INFO {
		fmt.Println(utils.TimeNowAsRFC3339NanoString() + loglevels.InfoPrefix + message)
	}
}

func (cl *ConsoleLogger) Warn(message string, errs ...error) {
	if cl.level <= loglevels.WARN {
		fmt.Println(utils.TimeNowAsRFC3339NanoString()+loglevels.WarnPrefix+message, errs)
	}
}

func (cl *ConsoleLogger) Error(message string, errs ...error) {
	if cl.level <= loglevels.ERROR {
		fmt.Println(utils.TimeNowAsRFC3339NanoString()+loglevels.ErrorPrefix+message, errs)
	}
}

func (cl *ConsoleLogger) Close() error {
	return nil
}
