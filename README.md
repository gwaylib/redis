# A redis integrated gadget

Depends on redis 5.0.3

## Examples

### Store long-term data
```golang
r, err := redis.NewRediStoreWithURL(context.TODO, 10, "redis://user:secret@localhost:6379/0?foo=bar&qux=baz")
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
r, err := redis.NewRediStoreWithURL(context.TODO, 10, "redis://user:secret@localhost:6379/0?foo=bar&qux=baz")
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
r, err := redis.NewRediStoreWithURL(context.TODO, 10, "redis://user:secret@localhost:6379/0?foo=bar&qux=baz")
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
    r, err := redis.NewRediStoreWithURL(context.TODO, 10, "redis://user:secret@localhost:6379/0?foo=bar&qux=baz")
	if err != nil {
		log.Fatal(err)
	}
	defer r.Close()

	p := NewMsqProducer(r, streamName)
	if err := p.Put("msg title", []byte("msg body")); err != nil {
		log.Fatal(err)
	}

	delayOverdue := 5 * time.Minute
	consumer, err := NewMsqConsumer(context.TODO(),
		r,
		streamName,
        "0", // if you have a multiply reader, you need set a different client id.
		delayOrverdue,
	)
	if err != nil {
		log.Fatal(err)
	}

	handle := func(id string, e *redis.FieldEntry) bool {
		// TODO: handle something
		//return false // wait next

		fmt.Println(id, *e) // 1762218531415-0 {key [98 111 100 121]}
		return true // ack for delete
	}

	// consume
    // loop block, here is for testing, it should be ran with goroutine.
    for {
        if err := consumer.Next(handle); err != nil{
            log.Warn(errors.As(err))
            time.Sleep(time.Second)
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
