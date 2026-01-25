package msq

import "github.com/gwaylib/redis"

type MsqProducer interface {
	Close() error
	Len() (int64, error) // return the length of queue
	Put(taskKey string, taskData []byte) error
}

type redisMsqProducer struct {
	rs *redis.RediStore

	streamName string
}

func NewMsqProducer(rs *redis.RediStore, streamName string) MsqProducer {
	p := &redisMsqProducer{rs: rs, streamName: streamName}
	return p
}

func (p *redisMsqProducer) Close() error {
	return p.rs.Close()
}

func (p *redisMsqProducer) Len() (int64, error) {
	return p.rs.XLen(p.streamName)
}

func (p *redisMsqProducer) Put(taskKey string, taskData []byte) error {
	if _, err := p.rs.XAdd(p.streamName, taskKey, taskData); err != nil {
		return err
	}
	return nil
}
