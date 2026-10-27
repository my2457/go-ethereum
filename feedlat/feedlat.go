// Package feedlat measures in-node latency from sequencer-feed receipt to
// websocket publication of a block, broken into per-step segments. Stamp
// points (each fires at most once per message/block, never inside a loop
// over transactions):
//
//	StampFeed     broadcast client: feed frame read off the socket
//	StampVerified broadcast client: frame signatures verified
//	StampStreamed broadcast client: messages accepted by the tx streamer
//	StampExecStart execution engine: block production begins (by block num)
//	StampExecDone  execution engine: block produced (binds num -> hash)
//	StampHead      core blockchain: canonical head event fired
//	StampWS        eth filters: head dispatched to WS subscribers (finalize)
//
// A background goroutine logs percentile windows every 10s. Unjoined
// stamps are pruned after 2 minutes.
package feedlat

import (
	"sort"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/log"
)

type entry struct {
	feed, verified, streamed, execStart, execDone, head time.Time
	batchW, committed, headW, ws                        time.Time
}

var (
	mu             sync.Mutex
	byHash         = make(map[common.Hash]*entry)
	execStartByNum = make(map[uint64]time.Time)
	samples        [][10]time.Duration // seg durations, -1 = pair missing
	unjoined       int
	once           sync.Once
)

func get(hash common.Hash) *entry {
	e, ok := byHash[hash]
	if !ok {
		e = &entry{}
		byHash[hash] = e
	}
	return e
}

func StampFeed(hash common.Hash, t time.Time) {
	if hash == (common.Hash{}) {
		return
	}
	once.Do(startLogger)
	mu.Lock()
	if e := get(hash); e.feed.IsZero() {
		e.feed = t
	}
	mu.Unlock()
}

func StampVerified(hash common.Hash, t time.Time) {
	if hash == (common.Hash{}) {
		return
	}
	mu.Lock()
	if e := get(hash); e.verified.IsZero() {
		e.verified = t
	}
	mu.Unlock()
}

func StampStreamed(hash common.Hash, t time.Time) {
	if hash == (common.Hash{}) {
		return
	}
	mu.Lock()
	if e := get(hash); e.streamed.IsZero() {
		e.streamed = t
	}
	mu.Unlock()
}

func StampExecStart(blockNum uint64, t time.Time) {
	mu.Lock()
	if _, ok := execStartByNum[blockNum]; !ok {
		execStartByNum[blockNum] = t
	}
	mu.Unlock()
}

func StampExecDone(hash common.Hash, blockNum uint64, t time.Time) {
	if hash == (common.Hash{}) {
		return
	}
	mu.Lock()
	e := get(hash)
	if e.execDone.IsZero() {
		e.execDone = t
	}
	if es, ok := execStartByNum[blockNum]; ok {
		if e.execStart.IsZero() {
			e.execStart = es
		}
		delete(execStartByNum, blockNum)
	}
	mu.Unlock()
}

func StampHead(hash common.Hash, t time.Time) {
	if hash == (common.Hash{}) {
		return
	}
	once.Do(startLogger)
	mu.Lock()
	if e := get(hash); e.head.IsZero() {
		e.head = t
	}
	mu.Unlock()
}

func seg(a, b time.Time) time.Duration {
	if a.IsZero() || b.IsZero() {
		return -1
	}
	return b.Sub(a)
}

func StampBatchWritten(hash common.Hash, t time.Time) {
	if hash == (common.Hash{}) {
		return
	}
	mu.Lock()
	if e := get(hash); e.batchW.IsZero() {
		e.batchW = t
	}
	mu.Unlock()
}

func StampStateCommitted(hash common.Hash, t time.Time) {
	if hash == (common.Hash{}) {
		return
	}
	mu.Lock()
	if e := get(hash); e.committed.IsZero() {
		e.committed = t
	}
	mu.Unlock()
}

func StampHeadWritten(hash common.Hash, t time.Time) {
	if hash == (common.Hash{}) {
		return
	}
	mu.Lock()
	if e := get(hash); e.headW.IsZero() {
		e.headW = t
	}
	mu.Unlock()
	tryFinalize(hash)
}

// tryFinalize emits the sample once feed, head-written and ws stamps all
// exist (their arrival order differs between the stock and early-publish
// paths) and drops the entry.
func tryFinalize(hash common.Hash) {
	mu.Lock()
	defer mu.Unlock()
	e, ok := byHash[hash]
	if !ok || e.feed.IsZero() || e.headW.IsZero() || e.ws.IsZero() {
		return
	}
	t := e.ws
	samples = append(samples, [10]time.Duration{
		seg(e.feed, e.verified),
		seg(e.verified, e.streamed),
		seg(e.streamed, e.execStart),
		seg(e.execStart, e.execDone),
		seg(e.execDone, e.batchW),
		seg(e.batchW, e.committed),
		seg(e.committed, e.headW),
		seg(e.headW, t),
		seg(e.head, t),
		seg(e.feed, t),
	})
	delete(byHash, hash)
}

func StampWS(hash common.Hash, t time.Time) {
	if hash == (common.Hash{}) {
		return
	}
	mu.Lock()
	e, ok := byHash[hash]
	if !ok {
		unjoined++
		mu.Unlock()
		return
	}
	if e.ws.IsZero() {
		e.ws = t
	}
	mu.Unlock()
	tryFinalize(hash)
}

func pct(sorted []float64, q float64) float64 {
	if len(sorted) == 0 {
		return -1
	}
	return sorted[int(q*float64(len(sorted)-1))]
}

var segNames = [10]string{"feed_verified", "verified_streamed", "streamed_execstart", "exec", "execdone_batchwrite", "batchwrite_commit", "commit_headwritten", "headwritten_ws", "head_ws", "feed_ws_total"}

func startLogger() {
	go func() {
		tick := time.NewTicker(10 * time.Second)
		defer tick.Stop()
		for range tick.C {
			mu.Lock()
			ss := samples
			samples = nil
			uj := unjoined
			unjoined = 0
			cutoff := time.Now().Add(-2 * time.Minute)
			for h, e := range byHash {
				first := e.feed
				if first.IsZero() {
					first = e.head
				}
				if !first.IsZero() && first.Before(cutoff) {
					delete(byHash, h)
				}
			}
			for n, t := range execStartByNum {
				if t.Before(cutoff) {
					delete(execStartByNum, n)
				}
			}
			pend := len(byHash)
			mu.Unlock()
			args := []interface{}{"n_total", len(ss), "unjoined_ws", uj, "pending", pend}
			for i, name := range segNames {
				var col []float64
				for _, s := range ss {
					if s[i] >= 0 {
						col = append(col, float64(s[i].Microseconds())/1000.0)
					}
				}
				sort.Float64s(col)
				args = append(args, name+"_n", len(col), name+"_p50", pct(col, 0.50), name+"_p95", pct(col, 0.95), name+"_p99", pct(col, 0.99))
			}
			log.Info("feedlat window", args...)
		}
	}()
}
