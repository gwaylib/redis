package redis

import (
	"fmt"
	"testing"
	"time"

	"github.com/gomodule/redigo/redis"
)

func TestXStreamACK(t *testing.T) {
	rd, err := NewRediStore(2, "tcp", "127.0.0.1:6379", "")
	if err != nil {
		t.Fatal(err)
	}
	defer rd.Close()
	streamName := "logs-stream"
	groupName := "group1"
	consumerName := "client1"
	blockTime := 1 * time.Second

	if err := rd.XCreateGroup(streamName, groupName); err != nil {
		if err != ErrDataExist {
			t.Fatal(err)
		}
		// pass for data exist
	}

	// when the group recreate, the history data has back.
	defer func() {
		if err := rd.XDestroyGroup(streamName, groupName); err != nil {
			t.Fatal(err)
		}
	}()

	// producer
	_, err = rd.XAdd(streamName, "key1", []byte("val1"))
	if err != nil {
		t.Fatal(err)
	}

	// consumer
	limit := 10
	entries, err := rd.XReadGroup(streamName, groupName, consumerName, limit, blockTime)
	if err != nil {
		if redis.ErrNil != err {
			t.Fatal(err)
		}
		// no message
	}

	// send the ack release confirm
	for _, e := range entries {
		for _, msg := range e.Messages {
			if _, err := rd.XDel(streamName, msg.ID); err != nil {
				t.Fatal(err)
			}
			if _, err := rd.XACK(streamName, groupName, msg.ID); err != nil {
				t.Fatal(err)
			}
		}
	}

	// get the pending time
	if pending, err := rd.XPending(streamName, groupName, 100); err != nil {
		if redis.ErrNil != err {
			t.Fatal(err)
		}
		// pass
	} else {
		fmt.Println(len(pending), len(entries))
	}
	xlen, err := rd.XLen(streamName)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(xlen)
}

func TestXStreamConsumer(t *testing.T) {
	rd, err := NewRediStore(2, "tcp", "127.0.0.1:6379", "")
	if err != nil {
		t.Fatal(err)
	}
	defer rd.Close()
	streamName := "logs-stream"
	streamSize := 10
	groupName := "group1"
	consumerName := "client1"
	blockTime := 1 * time.Second

	if err := rd.XCreateGroup(streamName, groupName); err != nil {
		if err != ErrDataExist {
			t.Fatal(err)
		}
		// pass for data exist
	}

	// when the group recreate, the history data has back.
	defer func() {
		if err := rd.XDestroyGroup(streamName, groupName); err != nil {
			t.Fatal(err)
		}
	}()

	// producer
	go func() {
		n := streamSize * 10
		for i := 0; i < n; i++ {
			size, err := rd.XLen(streamName)
			if err != nil {
				t.Fatal(err)
			}
			if size > streamSize {
				time.Sleep(200 * time.Millisecond)
				continue
			}
			_, err = rd.XAdd(streamName, "key1", []byte("val1"))
			if err != nil {
				t.Fatal(err)
			}
		}
	}()

	// consumer
consume:
	limit := 10
	entries, err := rd.XReadGroup(streamName, groupName, consumerName, limit, blockTime)
	if err != nil {
		if redis.ErrNil != err {
			t.Fatal(err)
		}
		// no message
	}

	// get the pending time
	if _, err := rd.XPending(streamName, groupName, 100); err != nil {
		if redis.ErrNil != err {
			t.Fatal(err)
		}
		// pass
	}

	// TODO: testing the xclaim

	// send the ack release confirm
	for _, e := range entries {
		for _, msg := range e.Messages {
			if _, err := rd.XDel(streamName, msg.ID); err != nil {
				t.Fatal(err)
			}
			//if _, err := rd.XACK(streamName, groupName, msg.ID); err != nil {
			//	t.Fatal(err)
			//}
		}
	}
	fmt.Printf("%+v\n", entries)
	if len(entries) != 0 {
		goto consume
	}
}
