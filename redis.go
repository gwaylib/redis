// Refere:
// https://github.com/gomodule/redigo/redis
// https://github.com/garyburd/redigo
// https://github.com/boj/redistore

package redis

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/gomodule/redigo/redis"
)

var (
	ErrNil       = redis.ErrNil
	ErrDataExist = errors.New("Data already exist")
)

// RediStore stores sessions in a redis backend.
type RediStore struct {
	Pool *redis.Pool
}

func dial(network, address, password string) (redis.Conn, error) {
	c, err := redis.Dial(network, address)
	if err != nil {
		return nil, err
	}
	if password != "" {
		if _, err := c.Do("AUTH", password); err != nil {
			c.Close()
			return nil, err
		}
	}
	return c, err
}

// NewRediStore returns a new RediStore.
// size: maximum number of idle connections.
func NewRediStore(size int, network, address, password string) (*RediStore, error) {
	return NewRediStoreWithPool(&redis.Pool{
		MaxIdle:     size,
		IdleTimeout: 240 * time.Second,
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			return err
		},
		Dial: func() (redis.Conn, error) {
			return dial(network, address, password)
		},
	})
}

func dialWithDB(network, address, password, DB string) (redis.Conn, error) {
	c, err := dial(network, address, password)
	if err != nil {
		return nil, err
	}
	if _, err := c.Do("SELECT", DB); err != nil {
		c.Close()
		return nil, err
	}
	return c, err
}

// NewRediStoreWithDB - like NewRedisStore but accepts `DB` parameter to select
// redis DB instead of using the default one ("0")
func NewRediStoreWithDB(size int, network, address, password, DB string) (*RediStore, error) {
	return NewRediStoreWithPool(&redis.Pool{
		MaxIdle:     size,
		IdleTimeout: 240 * time.Second,
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			return err
		},
		Dial: func() (redis.Conn, error) {
			return dialWithDB(network, address, password, DB)
		},
	})
}

func NewRediStoreWithURL(ctx context.Context, size int, rawurl string, options ...redis.DialOption) (*RediStore, error) {
	return NewRediStoreWithPool(&redis.Pool{
		MaxIdle:     size,
		IdleTimeout: 240 * time.Second,
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			return err
		},
		Dial: func() (redis.Conn, error) {
			return redis.DialURLContext(ctx, rawurl, options...)
		},
	})
}

// NewRediStoreWithPool instantiates a RediStore with a *redis.Pool passed in.
func NewRediStoreWithPool(pool *redis.Pool) (*RediStore, error) {
	rs := &RediStore{
		Pool: pool,
	}
	_, err := rs.ping()
	return rs, err
}

// Close closes the underlying *redis.Pool
func (s *RediStore) Close() error {
	return s.Pool.Close()
}

// ping does an internal ping against a server to check if it is alive.
func (s *RediStore) ping() (bool, error) {
	conn := s.Pool.Get()
	defer conn.Close()
	data, err := conn.Do("PING")
	if err != nil || data == nil {
		return false, err
	}
	return (data == "PONG"), nil
}

func (s *RediStore) Do(cmd string, args ...interface{}) (interface{}, error) {
	conn := s.Pool.Get()
	defer conn.Close()
	return conn.Do(cmd, args...)
}

// Delete removes the session from redis.
func (s *RediStore) Delete(key string) error {
	conn := s.Pool.Get()
	defer conn.Close()
	if _, err := conn.Do("DEL", key); err != nil {
		return err
	}
	return nil
}

// When the next corsor return is 0, it means that there is no more data
func (s *RediStore) ScanKey(cursor int64, pattern string, limit int) (int64, [][]byte, error) {
	conn := s.Pool.Get()
	defer conn.Close()
	val, err := redis.Values(conn.Do("SCAN", cursor, "MATCH", pattern, "COUNT", limit))
	if err != nil {
		return 0, nil, err
	}
	nextCursor, err := redis.Int64(val[0], nil)
	if err != nil {
		return 0, nil, err
	}
	result, err := redis.ByteSlices(val[1], nil)
	if err != nil {
		return 0, nil, err
	}
	if nextCursor == 0 && len(result) == 0 {
		return 0, result, ErrNil
	}
	return nextCursor, result, err
}

// save stores the session in redis.
// store data with json format
// age -- 0 for no expired.
func (s *RediStore) PutJSON(key string, val interface{}, age time.Duration) error {
	b, err := json.Marshal(val)
	if err != nil {
		return err
	}
	conn := s.Pool.Get()
	defer conn.Close()
	if err = conn.Err(); err != nil {
		return err
	}
	overdue := int64(age / time.Millisecond)
	if overdue > 0 {
		_, err = conn.Do("SET", key, b, "PX", overdue)
		return err
	}

	_, err = conn.Do("SET", key, b)
	return err
}

// scan the data to a json interface.
func (s *RediStore) ScanJSON(key string, val interface{}) error {
	conn := s.Pool.Get()
	defer conn.Close()
	if err := conn.Err(); err != nil {
		return err
	}
	reply, err := conn.Do("GET", key)
	if err != nil {
		return err
	}
	out, err := redis.Bytes(reply, err)
	if err != nil {
		return err
	}
	return json.Unmarshal(out, val)
}

func (s *RediStore) Get(key string) (interface{}, error) {
	conn := s.Pool.Get()
	defer conn.Close()
	if err := conn.Err(); err != nil {
		return nil, err
	}
	return conn.Do("GET", key)
}

func (s *RediStore) Put(key string, val interface{}, age time.Duration) error {
	conn := s.Pool.Get()
	defer conn.Close()
	if err := conn.Err(); err != nil {
		return err
	}
	var err error
	overdue := int64(age / time.Millisecond)
	if overdue > 0 {
		_, err = conn.Do("SET", key, val, "PX", overdue)
		return err
	}

	_, err = conn.Do("SET", key, val)
	return err
}
