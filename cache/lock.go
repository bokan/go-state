package cache

import (
	"context"
	"errors"
	"github.com/go-redis/redis/v8"
	"math/rand"
	"time"
)

var ErrTimeout = errors.New("timeout")

const (
	// DefaultLockExpiry is the default expiry for locks
	DefaultLockExpiry = 5 * time.Second
)

type Locker[K Stringer] interface {
	AcquireLocks(ctx context.Context, keys []K) ([]uint64, error)
	WaitForLocks(ctx context.Context, keys []K) error
	ReleaseLocks(ctx context.Context, keys []K, rnd []uint64) error
}

type RedisLocker[K Stringer] struct {
	Locker[K]
	r  redis.Cmdable
	ns string
}

func NewRedisLocker[K Stringer](r redis.Cmdable, namespace string) *RedisLocker[K] {
	return &RedisLocker[K]{r: r, ns: namespace}
}

var _ Locker[Stringer] = (*RedisLocker[Stringer])(nil)

func (l *RedisLocker[K]) lockRedisKey(key K) string {
	return "lock{" + l.ns + ":" + key.String() + "}"
}

func (l *RedisLocker[K]) AcquireLocks(ctx context.Context, keys []K) ([]uint64, error) {
	p := l.r.Pipeline()
	defer p.Close()
	rnd := make([]uint64, len(keys))
	for i, k := range keys {
		rnd[i] = rand.Uint64()
		p.SetNX(ctx, l.lockRedisKey(k), rnd, DefaultLockExpiry)
	}
	cmds, err := p.Exec(ctx)
	if err != nil {
		return nil, err
	}
	for i, cmd := range cmds {
		if !cmd.(*redis.BoolCmd).Val() {
			rnd[i] = 0
		}
	}
	return rnd, nil
}

func (l *RedisLocker[K]) WaitForLocks(ctx context.Context, keys []K) error {
	timeout := time.NewTimer(DefaultLockExpiry)
	defer timeout.Stop()

	for {

		p := l.r.Pipeline()
		defer p.Close()
		for _, k := range keys {
			p.Exists(ctx, l.lockRedisKey(k))
		}
		cmds, err := p.Exec(ctx)
		if err != nil {
			return err
		}
		allreleased := true
		for _, cmd := range cmds {
			if cmd.(*redis.IntCmd).Val() > 0 {
				allreleased = false
				break
			}
		}
		if allreleased {
			return nil
		}
		select {
		case <-timeout.C:
			return ErrTimeout
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func (l *RedisLocker[K]) ReleaseLocks(ctx context.Context, keys []K, rnd []uint64) error {
	p := l.r.Pipeline()
	defer p.Close()
	for _, k := range keys {
		// todo: user SCRIPT to compare and delete
		p.Del(ctx, l.lockRedisKey(k))
	}
	_, err := p.Exec(ctx)
	return err
}
