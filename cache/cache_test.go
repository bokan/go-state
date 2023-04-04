package cache_test

import (
	"context"
	"fmt"
	"github.com/bokan/go-state/cache"
	"github.com/stretchr/testify/assert"
	"log"
	"strconv"
	"testing"
)

type TestUserKey struct {
	PK int
}

func (t TestUserKey) String() string {
	return strconv.Itoa(t.PK)
}

type TestUser struct {
	Key  TestUserKey
	Name string
}

type TestUserMachinery struct {
	CacheAttemptIn  [2][]TestUserKey
	CacheAttemptOut [2][]*TestUser
	AcquireLocksOut []uint64
	WaitedForLocks  []TestUserKey
	ReleaseLocksIn  []TestUserKey
	LoadFromDBIn    []TestUserKey
	LoadFromDBOut   []*TestUser
}

func (m *TestUserMachinery) TryCache(ctx context.Context, keys []TestUserKey, attempt int) ([]*TestUser, error) {
	log.Println("TryCache", attempt)
	for _, k := range keys {
		log.Println("\t in", k.String())
		m.CacheAttemptIn[attempt] = append(m.CacheAttemptIn[attempt], k)
	}
	out := make([]*TestUser, len(keys))
	for i, k := range keys {
		if i%2 == 0 || attempt > 0 {
			out[i] = &TestUser{Key: k, Name: "User" + k.String()}
		}
	}
	for i, k := range keys {
		log.Println("\t out", k.String(), out[i])
		m.CacheAttemptOut[attempt] = append(m.CacheAttemptOut[attempt], out[i])
	}
	return out, nil
}

func (m *TestUserMachinery) AcquireLocks(ctx context.Context, keys []TestUserKey) ([]uint64, error) {
	log.Println("AcquireLocks")
	out := make([]uint64, len(keys))
	for i, k := range keys {
		log.Println("\t in", k.String())
		m.ReleaseLocksIn = append(m.ReleaseLocksIn, k)
		if i%2 == 0 {
			out[i] = 123
		}
	}
	for i, k := range keys {
		log.Println("\t out", k.String(), out[i])
		m.AcquireLocksOut = append(m.AcquireLocksOut, out[i])
	}
	return out, nil
}

func (m *TestUserMachinery) WaitForLocks(ctx context.Context, keys []TestUserKey) error {
	log.Println("WaitForLocks")
	for _, k := range keys {
		log.Println("\t", k.String())
		m.WaitedForLocks = append(m.WaitedForLocks, k)
	}
	return nil
}

func (m *TestUserMachinery) ReleaseLocks(ctx context.Context, keys []TestUserKey, rnd []uint64) error {
	log.Println("ReleaseLocks")
	for _, k := range keys {
		log.Println("\t", k.String())
		m.ReleaseLocksIn = append(m.ReleaseLocksIn, k)
	}
	return nil
}

func (m *TestUserMachinery) LoadFromDB(ctx context.Context, keys []TestUserKey) ([]*TestUser, error) {
	log.Println("LoadFromDB")
	out := make([]*TestUser, len(keys))
	for i, k := range keys {
		log.Println("\t", k.String())
		m.LoadFromDBIn = append(m.LoadFromDBIn, k)
		out[i] = &TestUser{Key: k, Name: "User" + k.String()}
		m.LoadFromDBOut = append(m.LoadFromDBOut, out[i])
	}
	return out, nil
}

func TestProcessor(t *testing.T) {
	m := &TestUserMachinery{}
	p := cache.NewProcessor[TestUserKey, TestUser](m, m, false)
	out, err := p.Process(context.Background(), []TestUserKey{{1}, {2}, {3}, {4}, {5}})
	assert.NoError(t, err)
	assert.Equal(t, m.CacheAttemptIn[0], []TestUserKey{{1}, {2}, {3}, {4}, {5}})
	assert.Equal(t, m.CacheAttemptOut[0], []*TestUser{
		&TestUser{Key: TestUserKey{1}, Name: "User1"},
		nil,
		&TestUser{Key: TestUserKey{3}, Name: "User3"},
		nil,
		&TestUser{Key: TestUserKey{5}, Name: "User5"},
	})
	assert.Equal(t, 5, len(out))
	assert.Equal(t, "User1", out[0].Name)
	assert.Equal(t, "User2", out[1].Name)
	assert.Equal(t, "User3", out[2].Name)
	assert.Equal(t, "User4", out[3].Name)
	assert.Equal(t, "User5", out[4].Name)

	fmt.Println(out)
}
