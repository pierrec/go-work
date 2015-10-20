// Package work eases concurrency patterns by providing often used helpers.
package work

import (
	"runtime"
	"sync"
	"sync/atomic"
)

// NumRoutines defines the maximum number of goroutines to be used per helper.
// It defaults to GOMAXPROCS.
var NumRoutines int

func init() {
	NumRoutines = runtime.GOMAXPROCS(0)
}

// do spawns workers with index 0 to n-1, limiting their numbers by NumRoutines.
func do(n int, worker func(int)) {
	var wg sync.WaitGroup
	if n <= NumRoutines {
		// spawn as many goroutines as number of workers
		wg.Add(n)
		for i := 0; i < n; i++ {
			go func(idx int) {
				worker(idx)
				wg.Done()
			}(i)
		}
		wg.Wait()
		return
	}

	// spawn the maximum number of goroutines
	wg.Add(NumRoutines)
	for i := 0; i < NumRoutines; i++ {
		go func(idx int) {
			for ; idx < n; idx += NumRoutines {
				worker(idx)
			}
			wg.Done()
		}(i)
	}
	wg.Wait()
}

// doWithError spawns workers with index 0 to n-1, limiting their numbers by NumRoutines.
// Similar to do but with error handling.
// The first error encountered aborts all processing and is then returned.
func doWithError(n int, worker func(int) error) error {
	var (
		errv atomic.Value // worker error
		wg   sync.WaitGroup
	)

	if n <= NumRoutines {
		// spawn as many goroutines as number of workers
		wg.Add(n)
		for i := 0; i < n; i++ {
			go func(idx int) {
				if errv.Load() == nil {
					if err := worker(idx); err != nil {
						errv.Store(err)
					}
				}
				wg.Done()
			}(i)
		}
		wg.Wait()

		if err := errv.Load(); err != nil {
			return err.(error)
		}
		return nil
	}

	// spawn the maximum number of goroutines
	wg.Add(NumRoutines)
	for i := 0; i < NumRoutines; i++ {
		go func(idx int) {
			for ; idx < n && errv.Load() == nil; idx += NumRoutines {
				if err := worker(idx); err != nil {
					errv.Store(err)
					break
				}
			}
			wg.Done()
		}(i)
	}
	wg.Wait()

	if err := errv.Load(); err != nil {
		return err.(error)
	}
	return nil
}

// Do spawns workers with index 0 to n-1, limiting their numbers by NumRoutines.
// If finalizer is set, then it is called on the processed items, in increasing index order.
func Do(n int, worker, finalizer func(idx int)) {
	switch n {
	case 0:
		return
	case 1:
		worker(0)
		if finalizer != nil {
			finalizer(0)
		}
		return
	}

	if finalizer == nil {
		do(n, worker)
		return
	}

	var (
		donec = make(chan struct{}, NumRoutines) // worker throttling
		workc = make(chan int)                   // results from workers
		wg    sync.WaitGroup
	)

	// initialize the go routine managing the results and
	// dispatching to the finalizer in order
	wg.Add(1)
	go func() {
		defer wg.Done()
		// buffer holds results that cannot be finalized yet.
		buffer := make(map[int]struct{})
		pos := 0
		for idx := range workc {
			if pos < idx {
				// out of order result
				buffer[idx] = struct{}{}
				continue
			}
			finalizer(idx)
			// process the results that were already received
			// ensuring they are processed in order
			for {
				pos++
				if pos == n {
					return
				}
				if _, ok := buffer[pos]; !ok {
					// no more result for the current position
					break
				}
				delete(buffer, pos)
				finalizer(pos)
			}
		}
	}()

	// process all items in the list, with a concurrency of NumRoutines
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(idx int) {
			worker(idx)
			workc <- idx
			<-donec
			wg.Done()
		}(i)
		// throttling
		donec <- struct{}{}
	}

	// done when all go routines are
	wg.Wait()

	return
}

// DoWithError spawns workers with index 0 to n-1, limiting their numbers by NumRoutines.
// Similar to Do but with error handling.
// The first error encountered aborts all processing and is then returned.
// If finalizer is set, then it is called on the processed items, in increasing index order.
func DoWithError(n int, worker, finalizer func(idx int) error) error {
	switch n {
	case 0:
		return nil
	case 1:
		if err := worker(0); err != nil {
			return err
		}
		if finalizer != nil {
			return finalizer(0)
		}
		return nil
	}

	if finalizer == nil {
		return doWithError(n, worker)
	}

	var (
		errv    atomic.Value                       // worker/finalizer error
		donec   = make(chan struct{}, NumRoutines) // worker done channel
		workc   = make(chan int)                   // results from workers
		wg, wgf sync.WaitGroup
	)

	// initialize the go routine managing the results and
	// dispatching to the finalizer in order
	wgf.Add(1)
	go func() {
		// buffer holds results that cannot be finalized yet.
		buffer := make(map[int]struct{})
		// current index to be processed
		pos := 0
		// the finalizer routine exits when the channel is closed
		// or when it has completed all work
		for idx := range workc {
			if errv.Load() != nil {
				// upon error, wait for the workers to complete
				// which will close the channel
				continue
			}
			if pos < idx {
				// out of order result
				buffer[idx] = struct{}{}
				continue
			}
			if err := finalizer(pos); err != nil {
				errv.Store(err)
				continue
			}
			// process the results that were already received
			// ensuring they are processed in order
			for {
				pos++
				if pos == n || errv.Load() != nil {
					break
				}
				if _, ok := buffer[pos]; !ok {
					// no more result for the current position
					break
				}
				delete(buffer, pos)
				if err := finalizer(pos); err != nil {
					errv.Store(err)
					break
				}
			}
		}
		wgf.Done()
	}()

	// process all items in the list, with a concurrency of NumRoutines
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(idx int) {
			if errv.Load() == nil {
				if err := worker(idx); err != nil {
					errv.Store(err)
				} else {
					workc <- idx
				}
			}
			<-donec
			wg.Done()
		}(i)
		// throttling
		donec <- struct{}{}
		if errv.Load() != nil {
			break
		}
	}

	// wait for workers
	wg.Wait()
	// since workc is blocking, the finalizer has received all items
	// so we can safely close it and shutdown the finalizer routine
	close(workc)

	// wait for finalizer
	wgf.Wait()

	if err := errv.Load(); err != nil {
		return err.(error)
	}
	return nil
}
