package model

import "time"

// Clock is the pipeline's only source of time. It is injected everywhere
// rather than calling time.Now directly, because replay (M9) and
// deterministic tests both need to control it, and retrofitting that later
// means touching every package.
type Clock interface {
	Now() time.Time
}

// SystemClock reads the host clock.
type SystemClock struct{}

func (SystemClock) Now() time.Time { return time.Now() }
