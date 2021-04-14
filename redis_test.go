package synlock

import (
	"runtime"
	"sync"
	"testing"
	"time"
)

func TestRedisMutex(t *testing.T) {
	r, err := NewRedis(DefRedisOpts)
	if err != nil {
		t.Fatalf("init redis object error: %s", err)
	}

	m1, err := r.NewMutex(123, 1)
	if err != nil {
		t.Fatalf("making new mutex error: %s", err)
	}
	mu1 := m1.(*RedisMutex)

	m2, err := r.NewMutex(123, 1)
	if err != nil {
		t.Fatalf("making new mutex error: %s", err)
	}
	mu2 := m2.(*RedisMutex)

	var critical = false
	if err = mu1.lock(); err != nil {
		t.Fatalf("acquiring lock error: %s", err)
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		if critical {
			t.Fatalf("invalid critical section value")
		}

		if err := mu2.lock(); err != nil {
			t.Fatalf("acquiring lock error: %s", err)
		}
		if !critical {
			t.Fatalf("unexpected access to the critical section")
		}
		if err := mu2.unlock(); err != nil {
			t.Fatalf("releasinglock error: %s", err)
		}
	}()

	runtime.Gosched()
	critical = true

	time.Sleep(time.Second)

	if err := mu1.unlock(); err != nil {
		t.Fatalf("releasing lock error: %s", err)
	}

	wg.Wait()
}
