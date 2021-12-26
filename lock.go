package redis

import (
	"errors"
	"time"

	"github.com/gomodule/redigo/redis"
)

func (s *RediStore) Lock(key, owner string, age int64) error {
	if age < 1 {
		return errors.New("age can not less than 1 second")
	}

	conn := s.Pool.Get()
	defer conn.Close()
	if err := conn.Err(); err != nil {
		return err
	}
	for {
		reply, err := conn.Do("GET", key)
		if err != nil {
			return err
		}
		if _, err := redis.String(reply, err); err != nil {
			if err != ErrNil {
				return err
			}
			// not exist
		} else {
			time.Sleep(time.Second / 5) // 200ms do a retry
			continue
		}
		if _, err = conn.Do("SETEX", key, age, owner); err != nil {
			return err
		}
		// locked.
		return nil
	}
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
