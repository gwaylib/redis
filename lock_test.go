package redis

import (
	"fmt"
	"testing"
	"time"
)

func TestRedisLock(t *testing.T) {
	r, err := NewRediStore(10, "tcp", "127.0.0.1:6379", "")
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	key := fmt.Sprintf("%d", time.Now().UnixNano())
	owner := "testing"
	if err := r.Lock(key, owner, time.Second); err != nil {
		t.Fatal(err)
	}
	if err := r.Unlock(key, owner); err != nil {
		t.Fatal(err)
	}

	// try a new lock, expect ok
	if err := r.Lock(key, owner+"1", time.Second); err != nil {
		t.Fatal(err)
	}
	// try a expired lock, will block the process
	// expect ok when lock expired
	if err := r.Lock(key, owner+"2", time.Second); err != nil {
		t.Fatal(err)
	}
	defer r.Unlock(key, owner+"2")
}
