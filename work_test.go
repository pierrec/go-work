package work_test

import (
	"fmt"
	"runtime"
	"testing"

	"github.com/pierrec/go-work"
)

var indexes []int

func init() {
	for i := 0; i < runtime.GOMAXPROCS(0)*2; i++ {
		indexes = append(indexes, i)
	}
}

// no error, no finalizer
func TestDo(t *testing.T) {
	for _, n := range indexes {
		results := make([]int, n)
		worker := func(idx int) {
			results[idx] = 1
		}
		work.Do(n, worker, nil)
		if m := count(results); m != n {
			t.Errorf("unexpected results size: got %d expected %d", m, n)
			t.FailNow()
		}
		for i := 0; i < n; i++ {
			if results[i] == 0 {
				t.Errorf("missing index %d", i)
				t.FailNow()
			}
		}
	}
}

// no error, finalizer
func TestDoFinalizer(t *testing.T) {
	for _, n := range indexes {
		results := make([]int, n)
		final := make([]int, n)
		worker := func(idx int) {
			results[idx] = 1
		}
		pos := 0
		finalizer := func(idx int) {
			pos++
			final[idx] = pos
		}
		work.Do(n, worker, finalizer)
		if m := count(results); m != n {
			t.Errorf("unexpected results size: got %d expected %d", m, n)
			t.FailNow()
		}
		if m := count(final); m != n {
			t.Errorf("unexpected final size: got %d expected %d", m, n)
			t.FailNow()
		}
		for i := 0; i < n; i++ {
			if final[i] == 0 {
				t.Errorf("missing index %d", i)
				t.FailNow()
			}
			if final[i] != i+1 {
				t.Errorf("finalizer ran out of order: %v", final)
				t.FailNow()
			}
		}
	}
}

// error, no finalizer
func TestDoWithoutError(t *testing.T) {
	for _, n := range indexes {
		results := make([]int, n)
		worker := func(idx int) error {
			results[idx] = 1
			return nil
		}
		err := work.DoWithError(n, worker, nil)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
			t.FailNow()
		}
		if m := count(results); m != n {
			t.Errorf("unexpected results size: got %d expected %d", m, n)
			t.FailNow()
		}
		for i := 0; i < n; i++ {
			if results[i] == 0 {
				t.Errorf("missing index %d", i)
				t.FailNow()
			}
		}
	}
}

func TestDoWithError(t *testing.T) {
	for _, n := range indexes {
		if n < 1 {
			continue
		}
		worker := func(idx int) error {
			if n == 1 || idx%2 > 0 {
				return fmt.Errorf("fail")
			}
			return nil
		}
		err := work.DoWithError(n, worker, nil)
		if err == nil {
			t.Errorf("expected error for n=%d", n)
			t.FailNow()
		}
	}
}

// error, finalizer
func TestDoFinalizerWithoutError(t *testing.T) {
	for _, n := range indexes {
		results := make([]int, n)
		final := make([]int, n)
		worker := func(idx int) error {
			results[idx] = 1
			return nil
		}
		pos := 0
		finalizer := func(idx int) error {
			pos++
			final[idx] = pos
			return nil
		}
		err := work.DoWithError(n, worker, finalizer)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
			t.FailNow()
		}
		if m := count(results); m != n {
			t.Errorf("unexpected results size: got %d expected %d", m, n)
			t.FailNow()
		}
		if m := count(final); m != n {
			t.Errorf("unexpected final size: got %d expected %d", m, n)
			t.FailNow()
		}
		for i := 0; i < n; i++ {
			if final[i] == 0 {
				t.Errorf("missing index %d", i)
				t.FailNow()
			}
			if final[i] != i+1 {
				t.Errorf("finalizer ran out of order: %v", final)
				t.FailNow()
			}
		}
	}
}

func TestDoFinalizerWithError(t *testing.T) {
	for _, n := range indexes {
		if n < 2 {
			continue
		}
		worker := func(idx int) error {
			return nil
		}
		finalizer := func(idx int) error {
			if idx%2 > 0 {
				return fmt.Errorf("fail")
			}
			return nil
		}
		err := work.DoWithError(n, worker, finalizer)
		if err == nil {
			t.Errorf("expected error")
			t.FailNow()
		}
	}
}

func TestDoFinalizerWithWorkerError(t *testing.T) {
	for _, n := range indexes {
		if n < 2 {
			continue
		}
		worker := func(idx int) error {
			if idx%2 > 0 {
				return fmt.Errorf("fail")
			}
			return nil
		}
		finalizer := func(idx int) error {
			return nil
		}
		err := work.DoWithError(n, worker, finalizer)
		if err == nil {
			t.Errorf("expected error")
			t.FailNow()
		}
	}
}

func count(l []int) int {
	n := 0
	for _, v := range l {
		if v != 0 {
			n++
		}
	}
	return n
}
