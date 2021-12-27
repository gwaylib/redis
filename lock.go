package redis

import (
	"errors"
	"time"

	"github.com/gomodule/redigo/redis"
)

func (s *RediStore) Lock(key, owner string, age time.Duration) error {
	if age < time.Millisecond {
		return errors.New("age can not less than 1 ms")
	}

	conn := s.Pool.Get()
	defer conn.Close()
	if err := conn.Err(); err != nil {
		return err
	}

	overdue := int64(age / time.Millisecond)
	for {
		reply, err := conn.Do("SET", key, owner, "NX", "PX", overdue)
		r, err := redis.String(reply, err)
		if err != nil {
			if err != redis.ErrNil {
				return err
			}
			// ErrNil
		} else if r == "OK" {
			return nil
		}

		time.Sleep(time.Second / 100) // 10ms do a retry
		continue
	}
	return nil
}
func (s *RediStore) Unlock(key, owner string) error {
	conn := s.Pool.Get()
	defer conn.Close()
	if err := conn.Err(); err != nil {
		return err
	}

	reply, err := conn.Do("GET", key)
	if err != nil {
		return err
	}
	storeOwner, err := redis.String(reply, err)
	if err != nil {
		if err != ErrNil {
			return err
		}
		// not exist
		return nil
	}
	if storeOwner != owner {
		return nil
	}
	if _, err := conn.Do("DEL", key); err != nil {
		return err
	}
	return nil
}
