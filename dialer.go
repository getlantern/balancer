package balancer

import (
	"math/rand"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/getlantern/ops"
)

var (
	// Proxy server may add a client IP address to blacklist if it constantly
	// makes connections without sending any request. Balancer will try to
	// avoid being blacklisted. Current Lantern server has a threshold of 10.
	serverBlacklistingThreshold int32 = 10

	nextCheckFactor = 10 * time.Second
)

// Dialer captures the configuration for dialing arbitrary addresses.
type Dialer struct {
	// Label: optional label with which to tag this dialer for debug logging.
	Label string

	// DialFN: this function dials the given network, addr.
	DialFN func(network, addr string) (net.Conn, error)

	// OnClose: (optional) callback for when this dialer is stopped.
	OnClose func()

	// Check: - a function that's used to test reachibility metrics
	// periodically or if the dialer was failed to connect.
	// It should return true for a successful check.
	//
	// Checks are scheduled at exponentially increasing intervals if dialer is
	// failed. Balancer will also schedule check when required.
	Check func() bool

	// Determines whether a dialer can be trusted with unencrypted traffic.
	Trusted bool

	// Modifies any HTTP requests made using connections from this dialer.
	OnRequest func(req *http.Request)
}

type dialer struct {
	emaDialTime *emaDuration

	*Dialer
	closeCh chan struct{}
	// prevent race condition when calling Timer.Reset()
	muCheckTimer sync.Mutex
	checkTimer   *time.Timer

	consecSuccesses int32
	consecFailures  int32
}

const longDuration = 100000 * time.Hour

func (d *dialer) Start() {
	d.consecSuccesses = 1 // be optimistic
	// assuming all dialers super fast initially
	// use large alpha to reflect network changes quickly
	d.emaDialTime = newEMADuration(0, 0.5)
	d.closeCh = make(chan struct{})
	d.checkTimer = time.NewTimer(longDuration)
	if d.Check == nil {
		d.Check = d.defaultCheck
	}

	ops.Go(func() {
		for {
			select {
			case <-d.closeCh:
				log.Tracef("Dialer %s stopped", d.Label)
				if d.OnClose != nil {
					d.OnClose()
				}
				return
			case <-d.checkTimer.C:
				go d.check()
			}
		}
	})
}

func (d *dialer) check() {
	log.Debugf("Start checking dialer %s", d.Label)
	t := time.Now()
	ok := d.Check()
	if ok {
		d.markSuccess()
		// Check time is generally larger than dial time, but still
		// meaningful when comparing latency across multiple
		// dialers.
		newEMA := d.emaDialTime.UpdateWith(time.Since(t))
		log.Tracef("Updated dialer %s emaDialTime to %v", d.Label, newEMA)
	} else {
		log.Tracef("Dialer %s failed check", d.Label)
		d.markFailure()
	}
}

func (d *dialer) Stop() {
	log.Tracef("Stopping dialer %s", d.Label)
	d.closeCh <- struct{}{}
}

func (d *dialer) EMADialTime() int64 {
	return d.emaDialTime.GetInt64()
}
func (d *dialer) ConsecSuccesses() int32 {
	return atomic.LoadInt32(&d.consecSuccesses)
}
func (d *dialer) ConsecFailures() int32 {
	return atomic.LoadInt32(&d.consecFailures)
}

func (d *dialer) dial(network, addr string) (net.Conn, error) {
	t := time.Now()
	conn, err := d.DialFN(network, addr)
	if err != nil {
		d.markFailure()
	} else {
		d.markSuccess()
		newEMA := d.emaDialTime.UpdateWith(time.Since(t))
		log.Tracef("Updated dialer %s emaDialTime to %v", d.Label, newEMA)
	}
	return conn, err
}

func (d *dialer) markSuccess() {
	newCS := atomic.AddInt32(&d.consecSuccesses, 1)
	log.Tracef("Dialer %s consecutive successes: %d -> %d", d.Label, newCS-1, newCS)
	// only when state is changing
	if newCS <= 2 {
		atomic.StoreInt32(&d.consecFailures, 0)
	}
}

func (d *dialer) markFailure() {
	newCF := atomic.AddInt32(&d.consecFailures, 1)
	log.Tracef("Dialer %s consecutive failures: %d -> %d", d.Label, newCF-1, newCF)
	// Don't bother to recheck if dialer is constantly failing.
	// Balancer will recheck when there's traffic after idle for some time.
	if newCF < serverBlacklistingThreshold/2 {
		atomic.StoreInt32(&d.consecSuccesses, 0)
		nextCheck := randomize(time.Duration(newCF*newCF) * nextCheckFactor)
		log.Debugf("Will recheck %s %v later because it failed for %d times", d.Label, nextCheck, newCF)
		d.muCheckTimer.Lock()
		d.checkTimer.Reset(nextCheck)
		d.muCheckTimer.Unlock()
	}
}

func (d *dialer) defaultCheck() bool {
	log.Errorf("No check function provided for dialer %s", d.Label)
	return true
}

// adds randomization to make requests less distinguishable on the network.
func randomize(d time.Duration) time.Duration {
	return time.Duration((d.Nanoseconds() / 2) + rand.Int63n(d.Nanoseconds()))
}
