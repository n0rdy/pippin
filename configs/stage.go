package configs

import (
	"github.com/n0rdy/pippin/logging"
	"time"
)

// StageConfig is a struct that holds the configuration for a stage.
//
// [StageConfig.MaxGoroutines] is the maximum number of goroutines that can be spawned within the stage.
// If it is passed as 0 or less, then there is no limit.
// This config option can be used to change the limit for each stage that comes from the [PipelineConfig.MaxGoroutinesPerStage] option (if provided).
//
// [StageConfig.Timeout] is the timeout for the stage.
// If it is passed as 0 or less, then there is no timeout.
//
// [StageConfig.CustomId] is a custom ID for the stage.
// If it is passed as 0, then the stage will be assigned an ID automatically.
// Auto-generated IDs are calculated as follows: 1 + the ID of the previous stage.
// The initial stage (the one that is created first) has an ID of 1.
// It is recommended to either rely on the auto-generated IDs or to provide a custom ID for each stage, otherwise the IDs might be messed up due to the (1 + the ID of the previous stage) logic mentioned above.
//
// [StageConfig.Logger] is a logger that will be used by the stage.
// If it is passed as nil, then the [logging.NoOpsLogger] logger will be used that does nothing.
// This config option can be used to change the logger for each stage that comes from the [PipelineConfig.Logger] option (if provided).
type StageConfig struct {
	MaxGoroutines int
	Timeout       time.Duration
	CustomId      int64
	Logger        logging.Logger
}
