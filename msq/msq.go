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

	streamName   string
	consumerName string
}

func NewRedisMsqConsumer(rs *redis.RediStore, streamName, consumerName string) (*RedisMsqConsumer, error) {
	c := &RedisMsqConsumer{rs: rs, streamName: streamName, consumerName: consumerName}
	// make the group name same with streamName
	if err := rs.XCreateGroup(streamName, streamName); err != nil {
		if redis.ErrDataExist != err {
			return nil, errors.As(err)
		}
	}
	return c, nil
}

func NewRedisClaimConsumer(ctx context.Context, rs *redis.RediStore, streamName string, claimDur time.Duration) (*RedisMsqConsumer, error) {
	mac, err := GetMAC()
	if err != nil {
		return nil, errors.As(err)
	}
	consumerName := fmt.Sprintf("%+v", mac)
	consumer, err := NewRedisMsqConsumer(rs, streamName, consumerName)
	if err != nil {
		return nil, errors.As(err)
	}
	go func() {
		for {
			select {
			case <-ctx.Done():
			default:
				if err := consumer.Claim(claimDur); err != nil {
					log.Println(errors.As(err))
				}
				time.Sleep(claimDur)
			}
		}
	}()
	return consumer, nil
}

func (c *RedisMsqConsumer) Next(limit int, timeout time.Duration) ([]redis.StreamEntry, error) {
	return c.rs.XReadGroup(c.streamName, c.streamName, c.consumerName, limit, timeout)
}

// confirm the message has deal, and delete it in the msq
// TODO: more testing on redis.XDEL
func (c RedisMsqConsumer) ACK(entryId string) error {
	if _, err := c.rs.XDel(c.streamName, entryId); err != nil {
		return err
	}
	if _, err := c.rs.XACK(c.streamName, c.streamName, entryId); err != nil {
		return err
	}
	return nil
}

// recover the dead client messages to self.
func (c *RedisMsqConsumer) Claim(overdue time.Duration) error {
	const limit = 10
emptyLoop:
	pending, err := c.rs.XPending(c.streamName, c.streamName, limit)
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
		if err := c.rs.XClaim(c.streamName, c.streamName, id, c.consumerName, overdue); err != nil {
			return err
		}
	}
	if len(pending) >= limit {
		goto emptyLoop
	}
	return nil
}
