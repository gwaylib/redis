// see PR: https://github.com/gomodule/redigo/pull/557/files
package redis

import (
	"errors"
	"time"

	"github.com/gomodule/redigo/redis"
)

type FieldEntry struct {
	Key   string
	Value []byte
}

type MessageEntry struct {
	ID     string
	Fields []FieldEntry
}

type StreamEntry struct {
	StreamName string
	Messages   []MessageEntry
}

func FieldEntries(replay interface{}, err error) ([]FieldEntry, error) {
	result, err := redis.Values(replay, nil)
	if err != nil {
		return nil, err
	}
	if len(result)%2 != 0 {
		return nil, errors.New("redigo: Entry expects 2x value field")
	}
	fields := make([]FieldEntry, len(result)/2)
	for i := 0; i < len(result)/2; i++ {
		fieldName, err := redis.String(result[i], nil)
		if err != nil {
			return nil, err
		}
		fieldValue, err := redis.Bytes(result[i+1], nil)
		if err != nil {
			return nil, err
		}
		fields[i] = FieldEntry{
			Key:   fieldName,
			Value: fieldValue,
		}
	}
	return fields, nil
}

// Need the replay is a message entry format.
func MessageEntries(reply interface{}, err error) ([]MessageEntry, error) {
	result, err := redis.Values(reply, err)
	if err != nil {
		return nil, err
	}

	entries := make([]MessageEntry, len(result))
	for i, r := range result {
		msg, err := redis.Values(r, nil)
		if len(msg) != 2 {
			return nil, errors.New("redigo: Entry expects two value of message")
		}
		msgId, err := redis.String(msg[0], nil)
		if err != nil {
			return nil, err
		}
		fields, err := FieldEntries(msg[1], nil)
		if err != nil {
			return nil, err
		}
		entries[i] = MessageEntry{
			ID:     msgId,
			Fields: fields,
		}
	}
	return entries, nil
}

func StreamEntries(reply interface{}, err error) ([]StreamEntry, error) {
	result, err := redis.Values(reply, err)
	if err != nil {
		return nil, err
	}
	entries := make([]StreamEntry, len(result))
	for i, r := range result {
		stream, err := redis.Values(r, nil)
		if len(stream) != 2 {
			return nil, errors.New("redigo: Entry expects two value of stream format")
		}
		streamName, err := redis.String(stream[0], nil)
		if err != nil {
			return nil, err
		}
		streamMsgs, err := MessageEntries(stream[1], nil)
		if err != nil {
			return nil, err
		}
		entries[i] = StreamEntry{
			StreamName: streamName,
			Messages:   streamMsgs,
		}
	}
	return entries, nil
}

// for stream xadd
// return the system id
func (s *RediStore) XAdd(streamName string, key string, val interface{}, kv ...interface{}) (string, error) {
	args := []interface{}{
		streamName, // stream_name
		"*",        // auto id
		key, val,
	}
	if len(kv) > 0 {
		args = append(args, kv...)
	}

	conn := s.Pool.Get()
	defer conn.Close()
	return redis.String(conn.Do("XADD", args...))
}

// capped the stream size
func (s *RediStore) XTrim(streamName string, maxLen int) (int, error) {
	args := []interface{}{
		streamName, // stream_name
		"MAXLEN", "~", maxLen,
	}

	conn := s.Pool.Get()
	defer conn.Close()
	return redis.Int(conn.Do("XTRIM", args...))
}

func (s *RediStore) XLen(streamName string) (int64, error) {
	conn := s.Pool.Get()
	defer conn.Close()
	return redis.Int64(conn.Do("XLEN", streamName))
}

func (s *RediStore) XDel(streamName, msgId string, otherIds ...string) (int, error) {
	args := make([]interface{}, len(otherIds)+2)
	args[0] = streamName
	args[1] = msgId
	for i, next := range otherIds {
		args[i] = next
	}

	conn := s.Pool.Get()
	defer conn.Close()
	return redis.Int(conn.Do("XDEL", args...))
}

//// for single read
//func (s *RediStore) XRead(streamName string, limit int, timeout time.Duration) ([]StreamEntry, error) {
//	conn := s.Pool.Get()
//	defer conn.Close()
//	return StreamEntries(conn.Do("XREAD", "BLOCK", int64(timeout/(1000*1000)), "COUNT", limit, "STREAMS", streamName, "0"))
//}

// TODO: confirm the MKSTREAM in which versoin
func (s *RediStore) XCreateGroup(streamName, groupName string) error {
	conn := s.Pool.Get()
	defer conn.Close()
	if _, err := conn.Do("XGROUP", "CREATE", streamName, groupName, 0, "MKSTREAM"); err != nil {
		if err.Error() == "BUSYGROUP Consumer Group name already exists" {
			return ErrDataExist
		}
		return err
	}
	return nil
}

func (s *RediStore) XDestroyGroup(streamName, groupName string) error {
	conn := s.Pool.Get()
	defer conn.Close()
	if _, err := conn.Do("XGROUP", "DESTROY", streamName, groupName); err != nil {
		return err
	}
	return nil
}
func (s *RediStore) XDeleteGroupUser(streamName, groupName, consumerName string) error {
	conn := s.Pool.Get()
	defer conn.Close()
	if _, err := conn.Do("XGROUP", "DELCONSUMER", streamName, groupName, consumerName); err != nil {
		return err
	}
	return nil
}

// for multi-consumer
// the group name is set as the stream name
// Need create first before using.
func (s *RediStore) XReadGroup(streamName, groupName, consumerName string, limit int, timeout time.Duration) ([]StreamEntry, error) {
	conn := s.Pool.Get()
	defer conn.Close()
	entries, err := StreamEntries(conn.Do("XREADGROUP", "GROUP", groupName, consumerName, "COUNT", limit, "STREAMS", streamName, "0"))
	if err != nil {
		if err != redis.ErrNil {
			return nil, err
		}
		// nodata, goto block
	} else {
		for _, msg := range entries {
			if len(msg.Messages) > 0 {
				return entries, nil
			}
		}
		// nodata, goto block
	}

	// goto the block mode
	return StreamEntries(conn.Do("XREADGROUP", "GROUP", groupName, consumerName, "BLOCK", int64(timeout/1e6), "COUNT", limit, "STREAMS", streamName, ">"))
}

// for multi-consumer
// the group name is set as the stream name
// Need create first before using.
func (s *RediStore) XReadGroupNonBlock(streamName, groupName, consumerName string, limit int, timeout time.Duration) ([]StreamEntry, error) {
	conn := s.Pool.Get()
	defer conn.Close()
	entries, err := StreamEntries(conn.Do("XREADGROUP", "GROUP", groupName, consumerName, "COUNT", limit, "STREAMS", streamName, "0"))
	if err != nil {
		if err != redis.ErrNil {
			return nil, err
		}
		// nodata, goto block
	} else {
		for _, msg := range entries {
			if len(msg.Messages) > 0 {
				return entries, nil
			}
		}
		// nodata, goto block
	}

	entries, err = StreamEntries(conn.Do("XREADGROUP", "GROUP", groupName, consumerName, "COUNT", limit, "STREAMS", streamName, ">"))
	if err != nil {
		return nil, err
	} else {
		for _, msg := range entries {
			if len(msg.Messages) > 0 {
				return entries, nil
			}
		}
		// nodata, goto block
	}
	return nil, ErrNil
}

func (s *RediStore) XACK(streamName, groupName, entryId string) (int, error) {
	conn := s.Pool.Get()
	defer conn.Close()
	return redis.Int(conn.Do("XACK", streamName, groupName, entryId))
}

// get the pending task.
// Since version 6.2 it is possible to filter entries by their idle-time,
func (s *RediStore) XPending(streamName, groupName string, limit int64) ([]interface{}, error) {
	conn := s.Pool.Get()
	defer conn.Close()
	// TODO:[id:val[0];consumerName:val[1];process duration(ms):val[2];try times:val[3]]
	return redis.Values(conn.Do("XPENDING", streamName, groupName, "-", "+", limit))
}

// transfer the timeout task to another consumer.
// XAUTOCLAIM Available since 6.2.0.
func (s *RediStore) XClaim(streamName, groupName, entryId, toConsumerName string, overDuration time.Duration) error {
	conn := s.Pool.Get()
	defer conn.Close()
	// the redis only support ms second
	if _, err := conn.Do("XCLAIM", streamName, groupName, toConsumerName, int64(overDuration/1e6), entryId); err != nil {
		return err
	}
	return nil
}
