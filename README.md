# A redis integrated gadget

Depends on redis 5.0.3

## Examples

### Store long-term data
```golang
r, err := redis.NewRediStore(10, "tcp", "127.0.0.1:6379", "")
if err != nil {
	t.Fatal(err)
}
defer r.Close()

id := 1
if err := r.PutJSON("testing", id, 0); err != nil {
	t.Fatal(err)
}
defer func(){
    if err := r.Delete("testing"); err != nil {
    	t.Fatal(err)
    }
}()

outId := 0
if err := r.ScanJSON("testing", &outId); err != nil {
	t.Fatal(err)
}
```

### Store timed data
```golang
r, err := redis.NewRediStore(10, "tcp", "127.0.0.1:6379", "")
if err != nil {
	t.Fatal(err)
}
defer r.Close()

id := 1
timeout := 60 // seconds
if err := r.PutJSON("testing", id, timeout); err != nil {
	t.Fatal(err)
}

outId := 0
if err := r.ScanJSON("testing", &outId); err != nil {
	t.Fatal(err)
}
```

### Distributed lock
```golang
r, err := redis.NewRediStore(10, "tcp", "127.0.0.1:6379", "")
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
```golang
	streamName := "logs-stream"
	r, err := redis.NewRediStore(10, "tcp", "127.0.0.1:6379", "")
	if err != nil {
		log.Fatal(err)
	}
	defer r.Close()

	p := NewRedisMsqProducer(r, streamName)
	if err := p.Put("msg title", []byte("msg body")); err != nil {
		log.Fatal(err)
	}

	overdue := 5 * time.Minute

	consumer, err := NewRedisAutoConsumer(context.TODO(), RedisAutoConsumerCfg{
		Redis:         r,
		StreamName:    streamName,
		ClaimDuration: overdue,
	})
	if err != nil {
		log.Fatal(err)
	}

	handle := func(m *redis.MessageEntry) bool {
		// TODO: handle something
		//return false
		return true
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
				if ok := handle(&msg); !ok {
					if err := consumer.Delay(&msg); err != nil {
						log.Println(errors.As(err, msg))
					}
					continue
				} else {
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
