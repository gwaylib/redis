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
	if err := r.Lock(key, owner, 60); err != nil {
		t.Fatal(err)
	}
	defer r.Unlock(key, owner)

}
