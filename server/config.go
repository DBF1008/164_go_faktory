package server

import (
	"time"

	"github.com/contribsys/faktory/manager"
	"github.com/contribsys/faktory/util"
)

// This is the ultimate scalability limitation in Faktory,
// we only allow this many connections to Redis.
var DefaultMaxPoolSize uint64 = 2000

type ServerOptions struct {
	GlobalConfig     map[string]any
	Binding          string
	StorageDirectory string
	RedisSock        string
	ConfigDirectory  string
	Environment      string
	Password         string //gosec:disable
	PoolSize         uint64
}

func (so *ServerOptions) String(subsys string, key string, defval string) string {
	val := so.Config(subsys, key, defval)
	str, ok := val.(string)
	if !ok {
		util.Warnf("Config error: %s/%s is not a String", subsys, key)
		return defval
	}
	return str
}

func (so *ServerOptions) Config(subsys string, key string, defval any) any {
	mapp, ok := so.GlobalConfig[subsys]
	if !ok {
		return defval
	}

	maps, ok := mapp.(map[string]any)
	if !ok {
		util.Warnf("Invalid configuration, expected a %s subsystem, using default", subsys)
		return defval
	}

	val, ok := maps[key]
	if !ok {
		return defval
	}
	return val
}

// DeadRetention resolves the dead set retention policy from configuration,
// falling back to the package defaults when unset or invalid.
//
//	[faktory]
//	  dead_timeout = "4320h"  # how long dead jobs are kept (Go duration, default 180d)
//	  dead_max_size = 10000   # max dead jobs retained; 0 disables the limit
func DeadRetention(opts *ServerOptions) (time.Duration, int64) {
	ttl := manager.DeadTTL
	if raw, ok := opts.Config("faktory", "dead_timeout", "").(string); ok && raw != "" {
		if d, err := time.ParseDuration(raw); err == nil {
			ttl = d
		} else {
			util.Warnf("Invalid faktory/dead_timeout %q, using default: %v", raw, err)
		}
	}

	maxSize := manager.DefaultDeadMaxSize
	switch n := opts.Config("faktory", "dead_max_size", nil).(type) {
	case int64:
		maxSize = n
	case int:
		maxSize = int64(n)
	case float64:
		maxSize = int64(n)
	case nil:
		// not configured, keep the default
	default:
		util.Warnf("Invalid faktory/dead_max_size %v, using default", n)
	}

	return ttl, maxSize
}
