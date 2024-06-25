package redsync

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"time"

	"github.com/go-redsync/redsync/v4/redis"
	"github.com/hashicorp/go-multierror"
)

// A DelayFunc is used to decide the amount of time to wait between retries.
type DelayFunc func(tries int) time.Duration

// A Mutex is a distributed mutual exclusion lock.
type Mutex struct {
	// 锁名称 锁关键字 所有对于锁的操作(获取、释放)都是对于这个锁进行的
	name   string
	// 锁过期时间 表示锁自动失效的时间长度 有助于防止死锁
	expiry time.Duration 
	// 获取锁的最大次数 当第一次尝试失败后 mutex 可以重试一定次数 
	tries     int 
	// 每次获取锁之间的延迟策略 
	delayFunc DelayFunc
	// 用于补偿网络延迟的时间
	driftFactor   float64
	// 用于锁续期时增加额外的时间 以防止锁意外过期
	timeoutFactor float64
	// 成功执行操作所需要的最小redis节点响应数量。默认为半数
	quorum int 
	// 生成锁值的函数。锁值是随机生层的，以保证锁的唯一性和安全性
	genValueFunc  func() (string, error)
	value         string
	// 锁预计过期时间。有助于在锁续期和判断锁状态时判断锁是否仍然有效
	until         time.Time
	// 是否在每次尝试获取锁之前打乱redis的节点的顺序。这样可以提高多个客户端同时尝试获取锁的公平性
	shuffle       bool
	failFast      bool
	// 在锁续期时是否采用Nx(not esixt)标志，如果为true，表示锁存在且未被其他客户端获取时才会续期
	setNXOnExtend bool
	// 一个redis连接池切片，包含所有用于redis操作的节点。这使得mutex可以额在多个redis节点上操作，提高系统的可用性和性能.
	pools []redis.Pool
}

// Name returns mutex name (i.e. the Redis key).
func (m *Mutex) Name() string {
	return m.name
}

// Value returns the current random value. The value will be empty until a lock is acquired (or WithValue option is used).
func (m *Mutex) Value() string {
	return m.value
}

// Until returns the time of validity of acquired lock. The value will be zero value until a lock is acquired.
func (m *Mutex) Until() time.Time {
	return m.until
}

// TryLock only attempts to lock m once and returns immediately regardless of success or failure without retrying.
func (m *Mutex) TryLock() error {
	return m.TryLockContext(context.Background())
}

// TryLockContext only attempts to lock m once and returns immediately regardless of success or failure without retrying.
func (m *Mutex) TryLockContext(ctx context.Context) error {
	return m.lockContext(ctx, 1)
}

// Lock locks m. In case it returns an error on failure, you may retry to acquire the lock by calling this method again.
func (m *Mutex) Lock() error {
	return m.LockContext(context.Background())
}

// LockContext locks m. In case it returns an error on failure, you may retry to acquire the lock by calling this method again.
func (m *Mutex) LockContext(ctx context.Context) error {
	return m.lockContext(ctx, m.tries)
}

// lockContext locks m. In case it returns an error on failure, you may retry to acquire the lock by calling this method again.
func (m *Mutex) lockContext(ctx context.Context, tries int) error {
	if ctx == nil {
		ctx = context.Background()
	}

	value, err := m.genValueFunc()
	if err != nil {
		return err
	}

	var timer *time.Timer
	for i := 0; i < tries; i++ {
		// 为0直接获取锁 不为0添加延迟等待 获取错误可重试
		if i != 0 {
			if timer == nil {
				timer = time.NewTimer(m.delayFunc(i))
			} else {
				timer.Reset(m.delayFunc(i))
			}

			select {
			case <-ctx.Done():
				timer.Stop()
				// Exit early if the context is done.
				return ErrFailed
			case <-timer.C:
				// Fall-through when the delay timer completes.
			}
		}

		start := time.Now()
		// 计算获取到锁的节点
		n, err := func() (int, error) {
			ctx, cancel := context.WithTimeout(ctx, time.Duration(int64(float64(m.expiry)*m.timeoutFactor)))
			defer cancel()
			return m.actOnPoolsAsync(func(pool redis.Pool) (bool, error) {
				return m.acquire(ctx, pool, value)
			})
		}()

		now := time.Now()
		// 计算预计的剩余过期时间
		until := now.Add(m.expiry - now.Sub(start) - time.Duration(int64(float64(m.expiry)*m.driftFactor)))
		// 超过半数获得所 and 锁已经释放 获取成功直接返回
		if n >= m.quorum && now.Before(until) {
			m.value = value
			m.until = until
			return nil
		}
		// 获取失败释放锁 进入下一次重试
		_, _ = func() (int, error) {
			ctx, cancel := context.WithTimeout(ctx, time.Duration(int64(float64(m.expiry)*m.timeoutFactor)))
			defer cancel()
			return m.actOnPoolsAsync(func(pool redis.Pool) (bool, error) {
				return m.release(ctx, pool, value)
			})
		}()
		if i == tries-1 && err != nil {
			return err
		}
	}

	return ErrFailed
}

// Unlock unlocks m and returns the status of unlock.
func (m *Mutex) Unlock() (bool, error) {
	return m.UnlockContext(context.Background())
}

// UnlockContext unlocks m and returns the status of unlock.
func (m *Mutex) UnlockContext(ctx context.Context) (bool, error) {
	// 释放锁
	n, err := m.actOnPoolsAsync(func(pool redis.Pool) (bool, error) {
		return m.release(ctx, pool, m.value)
	})
	// 小于规定的节点数目
	if n < m.quorum {
		return false, err
	}
	return true, nil
}

// Extend resets the mutex's expiry and returns the status of expiry extension.
func (m *Mutex) Extend() (bool, error) {
	return m.ExtendContext(context.Background())
}

// ExtendContext resets the mutex's expiry and returns the status of expiry extension.
// 重置锁的过期时间
// 使用锁保证特定时间内的独占访问，为了避免独占情况，会设置一个有效期。但是当一个进程阻塞，需要延长超时时间。
// ExtendContext 就是会更新锁的过期时间，并且会返回是否何止成功。
func (m *Mutex) ExtendContext(ctx context.Context) (bool, error) {
	start := time.Now()
	n, err := m.actOnPoolsAsync(func(pool redis.Pool) (bool, error) {
		// 延长超时时间 延长的时间为 
		return m.touch(ctx, pool, m.value, int(m.expiry/time.Millisecond))
	})
	// 设置的节点小于 允许的节点数目
	if n < m.quorum {
		return false, err
	}
	now := time.Now()
	until := now.Add(m.expiry - now.Sub(start) - time.Duration(int64(float64(m.expiry)*m.driftFactor)))
	// 计算是否达到等待过期时间 没有达到返回成功
	if now.Before(until) {
		m.until = until
		return true, nil
	}
	// 达到返回失败
	return false, ErrExtendFailed
}

// Valid returns true if the lock acquired through m is still valid. It may
// also return true erroneously if quorum is achieved during the call and at
// least one node then takes long enough to respond for the lock to expire.
//
// Deprecated: Use Until instead. See https://github.com/go-redsync/redsync/issues/72.
func (m *Mutex) Valid() (bool, error) {
	return m.ValidContext(context.Background())
}

// ValidContext returns true if the lock acquired through m is still valid. It may
// also return true erroneously if quorum is achieved during the call and at
// least one node then takes long enough to respond for the lock to expire.
//
// Deprecated: Use Until instead. See https://github.com/go-redsync/redsync/issues/72.
func (m *Mutex) ValidContext(ctx context.Context) (bool, error) {
	n, err := m.actOnPoolsAsync(func(pool redis.Pool) (bool, error) {
		return m.valid(ctx, pool)
	})
	return n >= m.quorum, err
}

func (m *Mutex) valid(ctx context.Context, pool redis.Pool) (bool, error) {
	if m.value == "" {
		return false, nil
	}
	conn, err := pool.Get(ctx)
	if err != nil {
		return false, err
	}
	defer conn.Close()
	reply, err := conn.Get(m.name)
	if err != nil {
		return false, err
	}
	return m.value == reply, nil
}

func genValue() (string, error) {
	b := make([]byte, 16)
	_, err := rand.Read(b)
	if err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(b), nil
}

func (m *Mutex) acquire(ctx context.Context, pool redis.Pool, value string) (bool, error) {
	conn, err := pool.Get(ctx)
	if err != nil {
		return false, err
	}
	defer conn.Close()
	// 对redis设置锁
	reply, err := conn.SetNX(m.name, value, m.expiry)
	if err != nil {
		return false, err
	}
	return reply, nil
}

// 保证原子性 
// 获取和删除一起操作
var deleteScript = redis.NewScript(1, `
	local val = redis.call("GET", KEYS[1])
	if val == ARGV[1] then
		return redis.call("DEL", KEYS[1])
	elseif val == false then
		return -1
	else
		return 0
	end
`)

func (m *Mutex) release(ctx context.Context, pool redis.Pool, value string) (bool, error) {
	conn, err := pool.Get(ctx)
	if err != nil {
		return false, err
	}
	defer conn.Close()
	status, err := conn.Eval(deleteScript, m.name, value)
	if err != nil {
		return false, err
	}
	// 等于-1 返回false
	if status == int64(-1) {
		return false, ErrLockAlreadyExpired
	}
	// 不等于0 返回true
	return status != int64(0), nil
}

// 用于实现锁的续期操作
// 判断KEYS[1]的值和传入的value是否具有一致(检测此客户端是否是合法的持有者)
   // 如果是合法持有者尝试续期
// 不是合法持有者 尝试使用setNx 对keys[1]设置过期时间为argv[2],锁的值为argv[1]
var touchWithSetNXScript = redis.NewScript(1, `
	if redis.call("GET", KEYS[1]) == ARGV[1] then 
		return redis.call("PEXPIRE", KEYS[1], ARGV[2])
	elseif redis.call("SET", KEYS[1], ARGV[1], "PX", ARGV[2], "NX") then
		return 1
	else
		return 0
	end
`)

var touchScript = redis.NewScript(1, `
	if redis.call("GET", KEYS[1]) == ARGV[1] then
		return redis.call("PEXPIRE", KEYS[1], ARGV[2])
	else
		return 0
	end
`)

func (m *Mutex) touch(ctx context.Context, pool redis.Pool, value string, expiry int) (bool, error) {
	conn, err := pool.Get(ctx)
	if err != nil {
		return false, err
	}
	defer conn.Close()

	touchScript := touchScript
	if m.setNXOnExtend {
		touchScript = touchWithSetNXScript
	}

	status, err := conn.Eval(touchScript, m.name, value, expiry)
	if err != nil {
		return false, err
	}
	return status != int64(0), nil
}

func (m *Mutex) actOnPoolsAsync(actFn func(redis.Pool) (bool, error)) (int, error) {
	type result struct {
		node     int
		statusOK bool
		err      error
	}

	ch := make(chan result, len(m.pools))
	// 在redis 池依次获取redis.Client执行相应操作
	// 记录执行的状态 
	for node, pool := range m.pools {
		go func(node int, pool redis.Pool) {
			r := result{node: node}
			r.statusOK, r.err = actFn(pool)
			ch <- r
		}(node, pool)
	}

	var (
		n     = 0
		taken []int
		err   error
	)

	for range m.pools {
		r := <-ch
		if r.statusOK {
			n++
		} else if r.err == ErrLockAlreadyExpired {
			err = multierror.Append(err, ErrLockAlreadyExpired)
		} else if r.err != nil {
			err = multierror.Append(err, &RedisError{Node: r.node, Err: r.err})
		} else {
			taken = append(taken, r.node)
			err = multierror.Append(err, &ErrNodeTaken{Node: r.node})
		}

		if m.failFast {
			// fast return
			if n >= m.quorum {
				return n, err
			}

			// fail fast
			if len(taken) >= m.quorum {
				return n, &ErrTaken{Nodes: taken}
			}
		}
	}
	// 执行失败的超过半数 返回失败
	if len(taken) >= m.quorum {
		return n, &ErrTaken{Nodes: taken}
	}
	// 返回执行成功的次数
	return n, err
}
