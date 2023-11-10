package msq

import (
	"context"
	"log"
	"time"

	redigo "github.com/gomodule/redigo/redis"
	"github.com/gwaylib/errors"
	"github.com/gwaylib/redis"
)

type MsqConsumerHandleFunc func(*redis.MessageEntry) bool

// https://redis.io/docs/data-types/streams/
// https://redis.io/commands/xclaim/
type MsqConsumer interface {
	Close() error

	// Read the next queue message
	Read(limit int, timeout time.Duration) ([]redis.StreamEntry, error)
	// Confirm and delete the queue message
	ACK(msgEntryId string) error
	// Put the message to the delay queue
	Delay(msgEntry *redis.MessageEntry) error

	// Read the queue msg, and send to the handleFn
	// if the handle function return true, auto send a ack to done the entry;
	// if the handle function return false, auto send the entry to a delay queue;
	// it will block the thread to wait the entry;
	// if a error happend, the caller need recall the Next function.
	Next(handleFn MsqConsumerHandleFunc) error
}

type redisMsqConsumer struct {
	rs *redis.RediStore

	mainStream    string
	delayStream   string
	delayDuration time.Duration
	clientId      string

	ctx  context.Context
	exit chan bool
}

func newMsqConsumer(ctx context.Context, rs *redis.RediStore, mainStream, clientId string, delayDuration time.Duration) (*redisMsqConsumer, error) {
	c := &redisMsqConsumer{
		rs:            rs,
		mainStream:    mainStream,
		delayStream:   mainStream + ".delay",
		delayDuration: delayDuration,
		clientId:      clientId,
		ctx:           ctx,
		exit:          make(chan bool, 3),
	}

	// make the group name same with mainStream
	if err := rs.XCreateGroup(mainStream, mainStream); err != nil {
		if redis.ErrDataExist != err {
			return nil, errors.As(err)
		}
	}
	if err := rs.XCreateGroup(c.delayStream, c.delayStream); err != nil {
		if redis.ErrDataExist != err {
			return nil, errors.As(err)
		}
	}
	return c, nil
}

// make a consumer wigth auto claim
// ctx, a context
// rs
//
func NewMsqConsumer(ctx context.Context, rs *redis.RediStore, streamName, clientId string, delayDuration time.Duration) (*redisMsqConsumer, error) {
	consumer, err := newMsqConsumer(ctx, rs, streamName, clientId, delayDuration)
	if err != nil {
		return nil, errors.As(err)
	}
	go func() {
		for {
			select {
			case <-consumer.exit:
			case <-consumer.ctx.Done():
			default:
				if err := consumer.Claim(delayDuration); err != nil {
					if redis.ErrNil != err {
						log.Println(errors.As(err))
					}
				}
				time.Sleep(delayDuration)
			}
		}
	}()
	return consumer, nil
}

func (c *redisMsqConsumer) Close() error {
	c.exit <- true // for read loop
	c.exit <- true // for claim loop
	c.exit <- true // for next loop
	return c.rs.Close()
}

func (c *redisMsqConsumer) Read(limit int, timeout time.Duration) ([]redis.StreamEntry, error) {
	for {
		select {
		case <-c.exit:
		case <-c.ctx.Done():
		default:
			result, err := c.rs.XReadGroup(c.mainStream, c.mainStream, c.clientId, limit, timeout)
			if err != nil {
				if err != redis.ErrNil {
					return nil, err
				}
				continue
			}
			return result, nil
		}
	}
}

func (c *redisMsqConsumer) ack(stream, entryId string) error {
	if _, err := c.rs.XDel(stream, entryId); err != nil {
		return err
	}
	if _, err := c.rs.XACK(stream, stream, entryId); err != nil {
		return err
	}
	return nil
}

// confirm the message has deal, and delete it in the msq
// TODO: more testing on redis.XDEL
func (c *redisMsqConsumer) ACK(msgEntryId string) error {
	return c.ack(c.mainStream, msgEntryId)
}

func (c *redisMsqConsumer) reQueue(fromStream, toStream string, entry *redis.MessageEntry) error {
	if len(entry.Fields) < 1 {
		return errors.New("need param pair with entry.Fields")
	}
	key := entry.Fields[0].Key
	val := entry.Fields[0].Value
	kv := []interface{}{}
	if len(entry.Fields) > 1 {
		for _, f := range entry.Fields[1:] {
			kv = append(kv, f.Key)
			kv = append(kv, f.Value)
		}
	}
	if _, err := c.rs.XAdd(toStream, key, val, kv...); err != nil {
		return err
	}
	return c.ack(fromStream, entry.ID)
}

// delay a message to delay stream, will ACK the message from the main stream.
// the delay stream will trigger back by c.Claim function
// the auto consumer will auto call the c.Clain, so it will also trigger back the delay message.
func (c *redisMsqConsumer) Delay(entry *redis.MessageEntry) error {
	return c.reQueue(c.mainStream, c.delayStream, entry)
}

func (c *redisMsqConsumer) delayBack(timeout time.Duration) error {
	limit := 10
	for {
		entries, err := c.rs.XReadGroup(c.delayStream, c.delayStream, c.clientId, limit, timeout)
		if err != nil {
			return err
		}
		for _, e := range entries {
			for _, m := range e.Messages {
				if err := c.reQueue(c.delayStream, c.mainStream, &m); err != nil {
					return err
				}
			}
		}
		if len(entries) < 10 {
			return nil
		}
	}
	return nil
}

// recover the dead client messages to self.
func (c *redisMsqConsumer) claim(overdue time.Duration) error {
	const limit = 10
emptyLoop:
	pending, err := c.rs.XPending(c.mainStream, c.mainStream, limit)
	if err != nil {
		return err
	}
	for _, epl := range pending {
		vals, err := redigo.Values(epl, nil)
		if err != nil {
			return err
		}
		if len(vals) != 4 {
			return errors.New("expect 4 values in epl")
		}
		id, err := redigo.String(vals[0], nil)
		if err != nil {
			return err
		}
		//consumerName, err := redigo.String(vals[1], nil)
		//if err != nil {
		//	return err
		//}
		processDur, err := redigo.Int(vals[2], nil)
		if err != nil {
			return err
		}
		if processDur < int(overdue/(1000*1000)) {
			// not reached the time
			continue
		}
		if err := c.rs.XClaim(c.mainStream, c.mainStream, id, c.clientId, overdue); err != nil {
			return err
		}
	}
	if len(pending) >= limit {
		goto emptyLoop
	}
	return nil
}

// recover the dead client messages to self.
func (c *redisMsqConsumer) Claim(overdue time.Duration) error {
	if err := c.claim(overdue); err != nil {
		return err
	}
	return c.delayBack(overdue)
}

func (c *redisMsqConsumer) Next(handleFn MsqConsumerHandleFunc) error {
	if handleFn == nil {
		panic("the handle function not set")
	}
	for {
		select {
		case <-c.exit:
		case <-c.ctx.Done():
		default:
			entries, err := c.Read(10, c.delayDuration)
			if err != nil {
				if err != redis.ErrNil {
					return errors.As(err)
				}
				// the server is still alive, keeping read
				continue
			}
			for _, e := range entries {
				for _, msg := range e.Messages {
					if ok := handleFn(&msg); !ok {
						if err := c.Delay(&msg); err != nil {
							return errors.As(err, msg)
						}
						continue
					} else {
						// confirm handle done.
						if err := c.ACK(msg.ID); err != nil {
							return errors.As(err, msg)
						}
					}
				}
			}
		}
	}
	return nil
}
