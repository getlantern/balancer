package balancer

import (
	"net"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/getlantern/withtimeout"
)

type Dialer struct {
	Weight int
	QOS    int
	Dial   func(network, addr string) (net.Conn, error)
	Test   func() bool
}

var (
	longDuration   = 1000000 * time.Hour
	maxTestTimeout = 1 * time.Minute
)

type dialer struct {
	*Dialer
	active int32
	errCh  chan error
}

func (d *dialer) start() {
	d.active = 1
	d.errCh = make(chan error, 1000)
	if d.Test == nil {
		d.Test = d.defaultTest
	}

	go func() {
		consecFailures := 0
		timer := time.NewTimer(longDuration)

		failed := func() {
			atomic.StoreInt32(&d.active, 0)
			consecFailures += 1
			timeout := time.Duration(consecFailures*consecFailures) * 100 * time.Millisecond
			if timeout > maxTestTimeout {
				timeout = maxTestTimeout
			}
			timer.Reset(timeout)
		}

		succeeded := func() {
			consecFailures = 0
			timer.Reset(longDuration)
		}

		for {
			select {
			case _, ok := <-d.errCh:
				if !ok {
					log.Trace("dialer stopped")
					return
				}
				failed()
			case <-timer.C:
				ok := d.Test()
				if ok {
					succeeded()
				} else {
					failed()
				}
			}
		}
	}()
}

func (d *dialer) isactive() bool {
	return atomic.LoadInt32(&d.active) == 1
}

func (d *dialer) onError(err error) {
	d.errCh <- err
}

func (d *dialer) stop() {
	close(d.errCh)
}

func (d *dialer) defaultTest() bool {
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
