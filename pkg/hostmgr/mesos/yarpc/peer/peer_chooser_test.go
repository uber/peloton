package peer

import (
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"go.uber.org/yarpc/transport/http"
)

func TestChooserInitialLookup(t *testing.T) {
	l := &url.URL{Host: "localhost:1234"}

	f := func(role string) (*url.URL, error) { return l, nil }

	c := NewPeerChooser(http.NewTransport(), 1*time.Second, f, "")
	assert.Equal(t, l, c.url.Load())

	p, _, err := c.Choose(nil, nil)
	assert.NoError(t, err)
	assert.Equal(t, l.Host, p.Identifier())
}

func TestChooserCallsDiscoverPeriodically(t *testing.T) {
	l := &url.URL{Host: "localhost:1234"}

	f := func(role string) (*url.URL, error) { return l, nil }

	c := NewPeerChooser(http.NewTransport(), 1*time.Millisecond, f, "")
	assert.Equal(t, l, c.url.Load())

	empty := &url.URL{}
	c.url.Store(empty)

	// Not started, url should not be updated.
	c.discover = func(role string) (*url.URL, error) {
		assert.Fail(t, "should not happen")
		return nil, nil
	}
	time.Sleep(25 * time.Millisecond)

	// Start and check the url is set.
	var wg sync.WaitGroup
	wg.Add(1)
	c.discover = func(role string) (*url.URL, error) {
		wg.Done()
		return l, nil
	}
	c.Start()
	wg.Wait()

	// Stop and check no url is set.
	c.Stop()
	c.discover = func(role string) (*url.URL, error) {
		assert.Fail(t, "should not happen")
		return nil, nil
	}
	time.Sleep(25 * time.Millisecond)
}
