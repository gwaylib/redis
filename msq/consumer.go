package msq

import (
	"context"
	"log"
	"time"

	redigo "github.com/gomodule/redigo/redis"
	"github.com/gwaylib/errors"
	"github.com/gwaylib/redis"
)

type MsqConsumerHandleFunc func(id string, entry *redis.FieldEntry) bool

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
	// call claim directly
	Claim(timeout time.Duration) error

	// Read the queue msg, and send to the handleFn
	// if the handle function return true, auto send a ack to done the entry;
	// if the handle function return false, auto send the entry to a delay queue;
	// it will block the thread to wait the entry;
	// if a error happend, the caller need recall the Next function.
	Next(handleFn MsqConsumerHandleFunc) error
	// message lenght
	// the first return is length of the normal queue, the second return is length of the delay quque
	Len() (int64, int64, error)
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
func NewMsqConsumer(ctx context.Context, rs *redis.RediStore, streamName, clientId string, delayDuration time.Duration) (*redisMsqConsumer, error) {
	consumer, err := newMsqConsumer(ctx, rs, streamName, clientId, delayDuration)
	if err != nil {
		return nil, errors.As(err)
	}
	go func() {
		delayTicker := time.NewTicker(delayDuration)
		for {
			select {
			case <-consumer.exit:
			case <-consumer.ctx.Done():
			case <-delayTicker.C:
				if err := consumer.Claim(delayDuration); err != nil {
					if !errors.Equal(redis.ErrNil, err) {
						log.Println(errors.As(err))
					}
				}
			}
		}
	}()
	return consumer, nil
}

func (c *redisMsqConsumer) Close() error {
	c.rs.Close()
	c.exit <- true // for read loop
	c.exit <- true // for claim loop
	c.exit <- true // for next loop
	return nil
}
func (c *redisMsqConsumer) Len() (int64, int64, error) {
	mainLen, err := c.rs.XLen(c.mainStream)
	if err != nil {
		return 0, 0, errors.As(err)
	}
	delayLen, err := c.rs.XLen(c.delayStream)
	if err != nil {
		return 0, 0, errors.As(err)
	}
	return mainLen, delayLen, nil
}

func (c *redisMsqConsumer) Read(limit int, timeout time.Duration) ([]redis.StreamEntry, error) {
	result, err := c.rs.XReadGroup(c.mainStream, c.mainStream, c.clientId, limit, timeout)
	if err != nil {
		return nil, errors.As(err)
	}
	return result, nil
}

func (c *redisMsqConsumer) ack(stream, entryId string) error {
	// 需要注意的是，XACK 只影响消费组的挂起列表，不会删除 Stream 中的数据本身。消息会继续在 Stream 容器中，直到你使用 XDEL 显式删除或使用 MAXLEN 超过限制被自动裁剪
	if _, err := c.rs.XDel(stream, entryId); err != nil {
		return errors.As(err)
	}
	if _, err := c.rs.XACK(stream, stream, entryId); err != nil {
		return errors.As(err)
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
		return errors.As(err)
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
	limit := 100
	for {
		entries, err := c.rs.XReadGroupNonBlock(c.delayStream, c.delayStream, c.clientId, limit, timeout)
		if err != nil {
			if !errors.Equal(err, redis.ErrNil) {
				return errors.As(err)
			}

			// no data for more
			return nil
		}
		for _, e := range entries {
			for _, m := range e.Messages {
				if err := c.reQueue(c.delayStream, c.mainStream, &m); err != nil {
					return errors.As(err)
				}
			}
		}
	}
}

// recover the dead client messages to self.
func (c *redisMsqConsumer) claim(streamName, groupName string, overdue time.Duration) error {
	const limit = 100
	pending, err := c.rs.XPending(streamName, groupName, limit)
	if err != nil {
		return err
	}
	for _, epl := range pending {
		vals, err := redigo.Values(epl, nil)
		if err != nil {
			return errors.As(err)
		}
		if len(vals) != 4 {
			return errors.New("expect 4 values in epl")
		}
		id, err := redigo.String(vals[0], nil)
		if err != nil {
			return errors.As(err)
		}
		consumerName, err := redigo.String(vals[1], nil)
		if err != nil {
			return err
		}
		processDur, err := redigo.Int(vals[2], nil)
		if err != nil {
			return errors.As(err)
		}
		if processDur < int(overdue/(1000*1000)) {
			// not reached the time
			continue
		}
		if consumerName != c.clientId {
			if err := c.rs.XClaim(streamName, groupName, id, c.clientId, overdue); err != nil {
				return errors.As(err)
			}
		}
	}
	return nil
}

// recover the dead client messages to self.
func (c *redisMsqConsumer) Claim(overdue time.Duration) error {
	mainLen, delayLen, err := c.Len()
	if err != nil {
		return errors.As(err)
	}
	if mainLen > 0 {
		if err := c.claim(c.mainStream, c.mainStream, overdue); err != nil {
			return errors.As(err)
		}
	}
	if delayLen > 0 {
		if err := c.claim(c.delayStream, c.delayStream, overdue); err != nil {
			return errors.As(err)
		}
		if err := c.delayBack(overdue); err != nil {
			return errors.As(err)
		}
	}
	return nil
}

func (c redisMsqConsumer) next(limit int, handleFn MsqConsumerHandleFunc) error {
	entries, err := c.Read(limit, c.delayDuration)
	if err != nil {
		if !errors.Equal(err, redis.ErrNil) {
			return errors.As(err)
		}
		// the server is still alive, keeping read
		return nil
	}
	for _, e := range entries {
		for _, msg := range e.Messages {
			if len(msg.Fields) != 1 {
				// not the producer protocal entry, delay it and need handle by others
				if err := c.Delay(&msg); err != nil {
					return errors.As(err, msg)
				}
				continue
			}
			if ok := handleFn(msg.ID, &msg.Fields[0]); !ok {
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

	return nil
}

func (c *redisMsqConsumer) Next(handleFn MsqConsumerHandleFunc) error {
	if handleFn == nil {
		panic("the handle function not set")
	}
	limit := 10
	for {
		select {
		case <-c.exit:
			return nil
		case <-c.ctx.Done():
			return nil
		default:
			if err := c.next(limit, handleFn); err != nil {
				if errors.Equal(redis.ErrNil, err) {
					continue
				}
				time.Sleep(time.Second)
				return errors.As(err)
			}
		}
	}
}
