package cache

import "context"

type Stringer interface {
	String() string
}

type Machinery[K Stringer, O any] interface {
	TryCache(ctx context.Context, keys []K, attempt int) ([]*O, error)
	LoadFromDB(ctx context.Context, keys []K) ([]*O, error)
}

type Processor[K Stringer, O any] struct {
	Machinery[K, O]
	Locker[K]
	readOnly bool
}

func NewProcessor[K Stringer, O any](m Machinery[K, O], l Locker[K], readOnly bool) *Processor[K, O] {
	return &Processor[K, O]{Machinery: m, Locker: l, readOnly: readOnly}
}

func (p *Processor[K, O]) Process(ctx context.Context, keys []K) ([]*O, error) {
	outMap := make(map[string]*O, len(keys))

	// Try to access data in cache
	cacheOut, err := p.TryCache(ctx, keys, 0)
	if err != nil {
		return nil, err
	}
	missing := []K{}
	for i, o := range cacheOut {
		if o != nil {
			outMap[keys[i].String()] = o
		} else {
			missing = append(missing, keys[i])
		}
	}

	if len(missing) > 0 {
		// Acquire locks
		locks, err := p.AcquireLocks(ctx, missing)
		if err != nil {
			return nil, err
		}
		gotLocks := []K{}
		cacheAgain := []K{}
		waitLocks := []K{}
		for i, lock := range locks {
			if lock > 0 {
				gotLocks = append(gotLocks, missing[i])
			} else {
				cacheAgain = append(cacheAgain, missing[i])
				waitLocks = append(waitLocks, missing[i])
			}
		}
		// Load data from DB
		if len(gotLocks) > 0 {
			dbData, err := p.LoadFromDB(ctx, gotLocks)
			_ = p.ReleaseLocks(ctx, gotLocks, locks)
			if err != nil {
				return nil, err
			}
			for i, o := range dbData {
				outMap[gotLocks[i].String()] = o
				if !p.readOnly {
					cacheAgain = append(cacheAgain, gotLocks[i])
				}
			}
		}

		if len(cacheAgain) > 0 {
			if len(waitLocks) > 0 {
				err = p.WaitForLocks(ctx, waitLocks)
				if err != nil {
					return nil, err
				}
			}
			// Try to access data in cache again
			cacheOut, err = p.TryCache(ctx, cacheAgain, 1)
			if err != nil {
				return nil, err
			}
			for i, o := range cacheOut {
				if o != nil {
					outMap[cacheAgain[i].String()] = o
				}
			}
		}
	}
	output := make([]*O, len(keys))
	for i, k := range keys {
		output[i] = outMap[k.String()]
	}
	return output, nil
}
