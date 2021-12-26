# A redis integrated gadget

Depends on redis 5.0.3

## Example for used:

### Store long-term data
```
	r, err := NewRediStore(10, "tcp", "127.0.0.1:6379", "")
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	id := 1
	if err := r.Set("testing", id, 0); err != nil {
		t.Fatal(err)
	}
    defer func(){
	    if err := r.Delete("testing"); err != nil {
	    	t.Fatal(err)
	    }
    }()

	outId := 0
	if err := r.Scan("testing", &outId); err != nil {
		t.Fatal(err)
	}
```

### Store timed data
```
	r, err := NewRediStore(10, "tcp", "127.0.0.1:6379", "")
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	id := 1
    timeout := 60 // seconds
	if err := r.Set("testing", id, timeout); err != nil {
		t.Fatal(err)
	}

	outId := 0
	if err := r.Scan("testing", &outId); err != nil {
		t.Fatal(err)
	}
```

### Distributed lock
```
	r, err := NewRediStore(10, "tcp", "127.0.0.1:6379", "")
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

    key := fmt.Sprintf("%d", time.Now().UnixNano())
    owner := "testing"
    if err := r.Lock(key, owner, 60); err != nil{
        t.Fatal(err)
    }
    defer r.Unlock(key, owner)
```

### Message queue
```
import (
	"fmt"
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

	mac, err := GetMAC()
	if err != nil {
		t.Fatal(err)
	}
	consumerName := fmt.Sprintf("%+v", mac)
	consumer, err := NewRedisMsqConsumer(r, streamName, consumerName)
	if err != nil {
		t.Fatal(err)
	}

	// auto claim, it will retry if no ack confirm after 5 minute.
	overdue := 5 * time.Minute
	go func() {
		for {
			if err := consumer.Claim(overdue); err != nil {
				log.Println(errors.As(err))
			}
			time.Sleep(overdue)
		}
	}()

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
```

## License

MIT License for part of lock/msq, For others, please refer to:  
```
https://github.com/gomodule/redigo/redis
https://github.com/garyburd/redigo
https://github.com/boj/redistore
```
