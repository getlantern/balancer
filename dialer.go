package balancer

import (
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/getlantern/withtimeout"
)

// Dialer captures the configuration for dialing arbitrary addresses.
type Dialer struct {
	// Weight: determines how often this Dialer is used relative to the other
	// Dialers on the balancer.
	Weight int

	// QOS: identifies the quality of service provided by this dialer. Higher
	// numbers equal higher quality. "Quality" in this case is loosely defined,
	// but can mean things such as reliability, speed, etc.
	QOS int

	// Dial: this function dials the given network, addr.
	Dial func(network, addr string) (net.Conn, error)

	// Check: (optional) - When dialing fails, this Dialer is deactivated (taken
	// out of rotation). Check is a function that's used to periodically after a
	// failed dial to check whether or not Dial works again. As soon as there is
	// a successful check, this Dialer will be activated (put back in rotation).
	//
	// If Check is not specified, a default Check will be used that makes an
	// HTTP request to http://www.google.com/humans.txt using this Dialer.
	//
	// Checks are scheduled at exponentially increasing intervals that are
	// capped at 1 minute.
	Check func() bool
}

var (
	longDuration    = 1000000 * time.Hour
	maxCheckTimeout = 1 * time.Minute
)

type dialer struct {
	*Dialer
	lastFailed         time.Time
	lastCheckSucceeded time.Time
	timeMutex          sync.RWMutex
	errCh              chan time.Time
}

func (d *dialer) start() {
	d.errCh = make(chan time.Time, 10)
	if d.Check == nil {
		d.Check = d.defaultCheck
	}

	go func() {
		consecCheckFailures := 0
		timer := time.NewTimer(longDuration)

		for {
			if d.failedAfterSuccessfulCheck() {
				log.Trace("Inactive, scheduling check")
				timeout := time.Duration(consecCheckFailures*consecCheckFailures) * 100 * time.Millisecond
				timer.Reset(timeout)
			}
			select {
			case t, ok := <-d.errCh:
				if !ok {
					log.Trace("dialer stopped")
					return
				}
				d.markFailed(t)
			case <-timer.C:
				ok := d.Check()
				if ok {
					d.markCheckSucceeded(time.Now())
					timer.Reset(longDuration)
				} else {
					consecCheckFailures += 1
				}
			}
		}
	}()
}

func (d *dialer) markFailed(t time.Time) {
	log.Tracef("Failed at: %s", t)
	d.timeMutex.Lock()
	d.lastFailed = t
	d.timeMutex.Unlock()
}

func (d *dialer) markCheckSucceeded(t time.Time) {
	log.Tracef("Check succeeded at: %s", t)
	d.timeMutex.Lock()
	d.lastCheckSucceeded = t
	d.timeMutex.Unlock()
}

func (d *dialer) isActive() bool {
	d.timeMutex.RLock()
	result := !d.failedAfterSuccessfulCheck()
	d.timeMutex.RUnlock()
	return result
}

func (d *dialer) failedAfterSuccessfulCheck() bool {
	return d.lastFailed.After(d.lastCheckSucceeded)
}

func (d *dialer) onError(err error) {
	select {
	case d.errCh <- time.Now():
		log.Trace("Error reported")
	default:
		log.Trace("There was already a pending error, ignoring new one")
	}
}

func (d *dialer) stop() {
	close(d.errCh)
}

func (d *dialer) defaultCheck() bool {
	client := &http.Client{
		Transport: &http.Transport{
			Dial: d.Dial,
		},
	}
	ok, timedOut, _ := withtimeout.Do(10*time.Second, func() (interface{}, error) {
		resp, err := client.Get("http://www.google.com/humans.txt")
		if err != nil {
			log.Tracef("Error on testing humans.txt: %s", err)
			return false, nil
		}
		resp.Body.Close()
		return resp.StatusCode == 200, nil
	})
	return !timedOut && ok.(bool)
}
