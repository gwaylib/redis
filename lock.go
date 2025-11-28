package redis

import (
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/gwaylib/errors"
)

var (
	ErrLocked   = errors.New("Another client has locked")
	ErrNotOwner = errors.New("Only onwer can be unlocked")
)

func (s *RediStore) Lock(key, owner string, age time.Duration) error {
	return s.lock(key, owner, age, true)
}
func (s *RediStore) TryLock(key, owner string, age time.Duration) error {
	return s.lock(key, owner, age, false)
}
func (s *RediStore) lock(key, owner string, age time.Duration, wait bool) error {
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

		if !wait {
			return ErrLocked.As(key, owner)
		}
		time.Sleep(time.Second / 100) // 10ms do a retry
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
		return ErrNotOwner
	}
	if _, err := conn.Do("DEL", key); err != nil {
		return err
	}
	return nil
}
