// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cpg1111/threadpool-go/blob/master/thread.go
// Provenance-includes-license: MIT
// Provenance-includes-copyright: Christian Grabowski

package indexheader

import (
	"runtime"
)

type execResult struct {
	value interface{}
	err   error
	// populated is only set to `true` by the start method of OSThread. The field only
	// exists so that we can tell when we've gotten a zero value of it. This is used by
	// the Call method so that we can detect when the thread is shutting down.
	populated bool
}

type OSThread struct {
	// stopping is a request for this thread to stop running.
	stopping chan struct{}

	// stopped is confirmation that the thread has stopped running.
	stopped chan struct{}

	// call is used to submit closures for this thread to execute.
	call chan func() (interface{}, error)

	// res is used to send the results of running closures back to callers.
	res chan execResult
}

func NewOSThread(stopping chan struct{}) *OSThread {
	return &OSThread{
		stopping: stopping,
		stopped:  make(chan struct{}),

		call: make(chan func() (interface{}, error), 1),
		res:  make(chan execResult, 1),
	}
}

func (o *OSThread) start() {
	runtime.LockOSThread()

	defer func() {
		runtime.UnlockOSThread()
		close(o.stopped)
	}()

	for {
		select {
		case <-o.stopping:
			// Close the channel used to read results so that any callers waiting on
			// results (which can happen if they submitted something before this method
			// has a chance to execute it but after this thread pool has been stopped)
			// so that they immediately get a zero value which they can check for.
			close(o.res)
			return
		case fn := <-o.call:
			val, err := fn()
			o.res <- execResult{value: val, err: err, populated: true}
			break
		}
	}
}

func (o *OSThread) Start() {
	go o.start()
}

// Join waits for the thread to complete any tasks it was executing after the
// stopping channel is closed by the parent thread pool. This allows callers to
// ensure the thread has completed executing which is useful in tests to verify
// no goroutines leak.
func (o *OSThread) Join() {
	<-o.stopped
}

func (o *OSThread) Call(fn func() (interface{}, error)) (interface{}, error) {
	select {
	case <-o.stopping:
		// Make sure the pool (and hence this thread) hasn't been stopped before
		// submitting our function to run. It's still possible for the thread pool
		// to be stopped between this check and our function actually being run by
		// the .start() method. We handle this by closing the o.res channel as part
		// of stopping this thread and testing for a zero value being returned
		// (which is what reading from a closed channel does).
		return nil, ErrPoolStopped
	case o.call <- fn:
		res := <-o.res
		if !res.populated {
			return nil, ErrPoolStopped
		}
		return res.value, res.err
	}
}
