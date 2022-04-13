// SPDX-License-Identifier: AGPL-3.0-only

package indexheader

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/grafana/mimir/pkg/util/test"
)

func TestOSThread_Call(t *testing.T) {
	t.Run("result channel already closed", func(t *testing.T) {
		test.VerifyNoLeak(t)

		stopping := make(chan struct{})
		thread := NewOSThread(stopping)

		// Don't start the thread but close the results channel. This ensures that we're testing
		// the case where the pool isn't shutdown yet, but we return a zero value to the caller.
		close(thread.res)
		res, err := thread.Call(func() (interface{}, error) {
			return 42, nil
		})

		assert.Nil(t, res)
		assert.ErrorIs(t, err, ErrPoolStopped)
	})

	t.Run("run by thread", func(t *testing.T) {
		test.VerifyNoLeak(t)

		stopping := make(chan struct{})
		thread := NewOSThread(stopping)
		t.Cleanup(func() {
			close(stopping)
			thread.Join()
		})

		thread.Start()
		res, err := thread.Call(func() (interface{}, error) {
			return 42, nil
		})

		assert.Equal(t, 42, res.(int))
		assert.NoError(t, err)
	})

	t.Run("run by thread but stopped", func(t *testing.T) {
		test.VerifyNoLeak(t)

		stopping := make(chan struct{})
		thread := NewOSThread(stopping)
		thread.Start()
		close(stopping)
		thread.Join()

		res, err := thread.Call(func() (interface{}, error) {
			return 42, nil
		})

		assert.Nil(t, res)
		assert.ErrorIs(t, err, ErrPoolStopped)
	})
}
