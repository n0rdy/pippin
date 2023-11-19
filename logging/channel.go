package logging

import (
	"fmt"
	"github.com/n0rdy/pippin/types/loglevels"
	"github.com/n0rdy/pippin/utils"
)

// ChannelLogger is a logger that writes to a channel.
// Please, make sure that you read from the channel, otherwise it will block the pipeline execution.
//
// ChannelLogger is handy when you want to write logs to a file or do some heavy actions on them, but don't want to affect performance of the pipeline.
//
// ChannelLogger accepts a log level as a parameter.
// It will print only those logs that have a level equal or higher than the specified one.
// Check [loglevels.LogLevel] for more details.
//
// Make sure to call the ChannelLogger.Close() method when you are done with the logger - this will close the channel.
type ChannelLogger struct {
	ch    chan<- string
	level loglevels.LogLevel
}

func NewChannelLogger(ch chan<- string, level loglevels.LogLevel) Logger {
	return &ChannelLogger{
		ch:    ch,
		level: level,
	}
}

func (cl *ChannelLogger) Trace(message string) {
	if cl.level <= loglevels.TRACE {
		cl.ch <- utils.TimeNowAsRFC3339NanoString() + loglevels.TracePrefix + message
	}
}

func (cl *ChannelLogger) Debug(message string) {
	if cl.level <= loglevels.DEBUG {
		cl.ch <- utils.TimeNowAsRFC3339NanoString() + loglevels.DebugPrefix + message
	}
}

func (cl *ChannelLogger) Info(message string) {
	if cl.level <= loglevels.INFO {
		cl.ch <- utils.TimeNowAsRFC3339NanoString() + loglevels.InfoPrefix + message
	}
}

func (cl *ChannelLogger) Warn(message string, errs ...error) {
	if cl.level <= loglevels.WARN {
		cl.ch <- utils.TimeNowAsRFC3339NanoString() + loglevels.WarnPrefix + cl.messageWithErrors(message, errs...)
	}
}

func (cl *ChannelLogger) Error(message string, errs ...error) {
	if cl.level <= loglevels.ERROR {
		cl.ch <- utils.TimeNowAsRFC3339NanoString() + loglevels.ErrorPrefix + cl.messageWithErrors(message, errs...)
	}
}

func (cl *ChannelLogger) Close() error {
	close(cl.ch)
	return nil
}

func (cl *ChannelLogger) messageWithErrors(message string, errs ...error) string {
	return fmt.Sprintf(message, errs)
}
