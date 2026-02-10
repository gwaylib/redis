package msq

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/gwaylib/redis"
)

func TestMsq(t *testing.T) {
	streamName := "logs-stream"
	r, err := redis.NewRediStore(10, "tcp", "127.0.0.1:6379", "")
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	overdue := 5 * time.Second

	consumer, err := newMsqConsumer(context.TODO(),
		r, streamName, "default", overdue,
	)
	if err != nil {
		t.Fatal(err)
	}

	ackHandle := func(id string, e *redis.FieldEntry) bool {
		fmt.Println("ack", id, *e) // 1762218531415-0 {key [98 111 100 121]}
		return true                // ack for delete
	}
	reQueueHandle := func(id string, e *redis.FieldEntry) bool {
		fmt.Println("requeue", id, *e) // 1762218531415-0 {key [98 111 100 121]}
		return false                   // ack for delete
	}
	// keep clean
	// handle by manully
	if err := consumer.Claim(1e9); err != nil {
		t.Fatal(err)
	}
	mainLen, delayLen, err := consumer.Len()
	if err != nil {
		t.Fatal(err)
	}
	if mainLen > 0 {
		if err := consumer.next(int(mainLen), ackHandle); err != nil {
			t.Fatal(err)
		}
	}

	p := NewMsqProducer(r, streamName)
	if err := p.Put("ack", []byte("body")); err != nil {
		t.Fatal(err)
	}
	if err := p.Put("delay", []byte("body")); err != nil {
		t.Fatal(err)
	}
	// consume
	// ack
	if err := consumer.next(1, ackHandle); err != nil {
		t.Fatal(err)
	}
	// delay
	if err := consumer.next(1, reQueueHandle); err != nil {
		t.Fatal(err)
	}
	// ack delay
	mainLen, delayLen, err = consumer.Len()
	if err != nil {
		t.Fatal(err)
	}
	_ = mainLen
	if delayLen != 1 {
		t.Fatalf("expect 1, but:%d", delayLen)
	}

	// handle by manully
	if err := consumer.Claim(1e9); err != nil {
		t.Fatal(err)
	}
	if err := consumer.next(30, ackHandle); err != nil {
		t.Fatal(err)
	}
	mainLen, delayLen, err = consumer.Len()
	if err != nil {
		t.Fatal(err)
	}
	if mainLen != 0 {
		t.Fatalf("expect mainStream len 0, but:%d", mainLen)
	}
	if delayLen != 0 {
		t.Fatalf("expect delayStream len 0, but:%d", delayLen)
	}
	consumer.Close()
}

func TestClaim(t *testing.T) {
	streamName := "contest-stream-room-create"
	r, err := redis.NewRediStore(10, "tcp", "127.0.0.1:6379", "xO3sU6jN1kN0bL2xT4pU0rM1")
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	overdue := 5 * time.Second

	consumer, err := newMsqConsumer(context.TODO(),
		r, streamName, "arena-node-1", overdue,
	)
	if err != nil {
		t.Fatal(err)
	}
	mainLen, delayLen, err := consumer.Len()
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println("before claim", mainLen, delayLen)
	// keep clean
	// handle by manully
	if err := consumer.Claim(1e9); err != nil {
		t.Fatal(err)
	}
	mainLen, delayLen, err = consumer.Len()
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println("after claim", mainLen, delayLen)

	consumer.Close()
}
