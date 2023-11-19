package utils

import "time"

func TimeNowAsRFC3339NanoString() string {
	return time.Now().Format(time.RFC3339Nano)
}
