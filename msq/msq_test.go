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
	r, err := redis.NewRediStore(10, "tcp", "127.0.0.1:6379", "pE91R5Chal1p3y3yRrQtJJ^M")
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	p := NewMsqProducer(r, streamName)
	if err := p.Put("msg title", []byte("msg body")); err != nil {
		t.Fatal(err)
	}

	overdue := 5 * time.Minute

	consumer, err := NewMsqConsumer(context.TODO(),
		r, streamName, "default", overdue,
	)
	if err != nil {
		t.Fatal(err)
	}

	handle := func(m *redis.MessageEntry) bool {
		// TODO: handle something
		//return false // wait next

		fmt.Println(*m) // {1762171946318-0 [{msg title [109 115 103 32 98 111 100 121]}]}
		return true     // ack
	}

	go func() {
		// consume
		if err := consumer.Next(handle); err != nil {
			fmt.Println(err)
		}
	}()
	time.Sleep(1e9)
	consumer.Close()
}
