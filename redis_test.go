package redis

import (
	"fmt"
	"testing"
	"time"
)

func TestRedisStore(t *testing.T) {
	r, err := NewRediStore(10, "tcp", "127.0.0.1:6379", "")
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	id := 1
	if err := r.PutJSON("testing", id, 60*time.Second); err != nil {
		t.Fatal(err)
	}
	outId := 0
	if err := r.ScanJSON("testing", &outId); err != nil {
		t.Fatal(err)
	}
	if id != outId {
		t.Fatal(id, outId)
	}

	nextCursor := int64(0)
	for {
		nextCursor, keys, err := r.ScanKey(nextCursor, "test*", 100)
		if err != nil {
			t.Fatal(err)
		}
		for _, k := range keys {
			fmt.Println(string(k))
		}
		if nextCursor == 0 {
			break
		}
	}

	if err := r.Delete("testing"); err != nil {
		t.Fatal(err)
	}
	if err := r.ScanJSON("testing", &outId); err != nil {
		if err != ErrNil {
			t.Fatal(err)
		}
	} else {
		t.Fatal("found data", outId)
	}
}
