package server

import (
	"testing"
	"time"

	"github.com/contribsys/faktory/manager"
	"github.com/stretchr/testify/assert"
)

func TestDeadRetention(t *testing.T) {
	t.Parallel()

	t.Run("defaults when unconfigured", func(t *testing.T) {
		opts := &ServerOptions{}
		ttl, maxSize := DeadRetention(opts)
		assert.Equal(t, manager.DeadTTL, ttl)
		assert.Equal(t, manager.DefaultDeadMaxSize, maxSize)
	})

	t.Run("custom timeout and max size", func(t *testing.T) {
		opts := &ServerOptions{GlobalConfig: map[string]any{
			"faktory": map[string]any{
				"dead_timeout":  "48h",
				"dead_max_size": int64(5),
			},
		}}
		ttl, maxSize := DeadRetention(opts)
		assert.Equal(t, 48*time.Hour, ttl)
		assert.EqualValues(t, 5, maxSize)
	})

	t.Run("zero max size means unlimited", func(t *testing.T) {
		opts := &ServerOptions{GlobalConfig: map[string]any{
			"faktory": map[string]any{"dead_max_size": int64(0)},
		}}
		_, maxSize := DeadRetention(opts)
		assert.EqualValues(t, 0, maxSize)
	})

	t.Run("invalid timeout falls back to default", func(t *testing.T) {
		opts := &ServerOptions{GlobalConfig: map[string]any{
			"faktory": map[string]any{"dead_timeout": "not-a-duration"},
		}}
		ttl, _ := DeadRetention(opts)
		assert.Equal(t, manager.DeadTTL, ttl)
	})
}
