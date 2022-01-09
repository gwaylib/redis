package msq

import (
	"context"
	"fmt"
	"log"
	"time"

	redigo "github.com/gomodule/redigo/redis"
	"github.com/gwaylib/errors"
	"github.com/gwaylib/redis"
)

type RedisMsqProducer struct {
	rs *redis.RediStore

	streamName string
}

func NewRedisMsqProducer(rs *redis.RediStore, streamName string) *RedisMsqProducer {
	p := &RedisMsqProducer{rs: rs, streamName: streamName}
	return p
}

func (p *RedisMsqProducer) Put(taskKey string, taskData []byte) error {
	if _, err := p.rs.XAdd(p.streamName, taskKey, taskData); err != nil {
		return err
	}
	return nil
}

type RedisMsqConsumer struct {
	rs *redis.RediStore

	mainStream  string
	delayStream string

	consumerName string
}
type RedisAutoConsumerCfg struct {
	Redis         *redis.RediStore
	StreamName    string
	ClientMemIdx  int // will set the client id with MAC_MemIdx
	ClaimDuration time.Duration
}

func NewRedisMsqConsumer(rs *redis.RediStore, mainStream, consumerName string) (*RedisMsqConsumer, error) {
	c := &RedisMsqConsumer{
		rs:           rs,
		mainStream:   mainStream,
		delayStream:  mainStream + "_delay",
		consumerName: consumerName,
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

func NewRedisAutoConsumer(ctx context.Context, cfg RedisAutoConsumerCfg) (*RedisMsqConsumer, error) {
	mac, err := GetMAC()
	if err != nil {
		return nil, errors.As(err)
	}
	consumerName := fmt.Sprintf("%+v_%d", mac, cfg.ClientMemIdx)
	consumer, err := NewRedisMsqConsumer(cfg.Redis, cfg.StreamName, consumerName)
	if err != nil {
		return nil, errors.As(err)
	}
	go func() {
		for {
			select {
			case <-ctx.Done():
			default:
				if err := consumer.Claim(cfg.ClaimDuration); err != nil {
					log.Println(errors.As(err))
				}
				time.Sleep(cfg.ClaimDuration)
			}
		}
	}()
	return consumer, nil
}

func (c *RedisMsqConsumer) Next(limit int, timeout time.Duration) ([]redis.StreamEntry, error) {
	return c.rs.XReadGroup(c.mainStream, c.mainStream, c.consumerName, limit, timeout)
}

func (c *RedisMsqConsumer) ack(stream, entryId string) error {
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
func (c *RedisMsqConsumer) ACK(entryId string) error {
	return c.ack(c.mainStream, entryId)
}

func (c *RedisMsqConsumer) reQueue(fromStream, toStream string, entry *redis.MessageEntry) error {
	if len(entry.Fields) < 2 {
		return errors.New("need param pair with entry.Fields")
	}
	key := entry.Fields[0].Key
	val := entry.Fields[1].Value
	kv := []interface{}{}
	if len(entry.Fields) > 2 {
		for _, f := range entry.Fields[2:] {
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
func (c *RedisMsqConsumer) Delay(entry *redis.MessageEntry) error {
	return c.reQueue(c.mainStream, c.delayStream, entry)
}

func (c *RedisMsqConsumer) delayBack(timeout time.Duration) error {
	limit := 10
	for {
		entries, err := c.rs.XReadGroup(c.delayStream, c.delayStream, c.consumerName, limit, timeout)
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
func (c *RedisMsqConsumer) claim(overdue time.Duration) error {
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
		if err := c.rs.XClaim(c.mainStream, c.mainStream, id, c.consumerName, overdue); err != nil {
			return err
		}
	}
	if len(pending) >= limit {
		goto emptyLoop
	}
	return nil
}

// recover the dead client messages to self.
func (c *RedisMsqConsumer) Claim(overdue time.Duration) error {
	if err := c.claim(overdue); err != nil {
		return err
	}
	return c.delayBack(overdue)
}
