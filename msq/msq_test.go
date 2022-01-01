package msq

import (
	"context"
	"log"
	"testing"
	"time"

	"github.com/gwaylib/errors"
	"github.com/gwaylib/redis"
)

func TestMsq(t *testing.T) {
	streamName := "logs-stream"
	r, err := redis.NewRediStore(10, "tcp", "127.0.0.1:6379", "")
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	p := NewRedisMsqProducer(r, streamName)
	if err := p.Put("msg title", []byte("msg body")); err != nil {
		t.Fatal(err)
	}

	overdue := 5 * time.Minute

	consumer, err := NewRedisClaimConsumer(context.TODO(), r, streamName, overdue)
	if err != nil {
		t.Fatal(err)
	}

	// consume
	limit := 10
	// for {
	for n := 0; n < 1; n++ {
		entries, err := consumer.Next(limit, overdue)
		if err != nil {
			if err != redis.ErrNil {
				log.Println(errors.As(err))
			}
			// the server is still alive, keeping read
			continue
		}
		for _, e := range entries {
			for _, msg := range e.Messages {

				// TODO: handle something

				// confirm handle done.
				if err := consumer.ACK(msg.ID); err != nil {
					log.Println(errors.As(err, msg))
					continue
				}
			}
		}
	}
}
