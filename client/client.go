// Copyright 2015 CoreOS, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package client

import (
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/coreos/etcd/Godeps/_workspace/src/golang.org/x/net/context"
)

var (
	ErrNoEndpoints           = errors.New("client: no endpoints available")
	ErrTooManyRedirects      = errors.New("client: too many redirects")
	errTooManyRedirectChecks = errors.New("client: too many redirect checks")
)

var (
	DefaultRequestTimeout   = 5 * time.Second
	DefaultAutosyncInterval = 10 * time.Minute
)

var DefaultTransport CancelableTransport = &http.Transport{
	Proxy: http.ProxyFromEnvironment,
	Dial: (&net.Dialer{
		Timeout:   30 * time.Second,
		KeepAlive: 30 * time.Second,
	}).Dial,
	TLSHandshakeTimeout: 10 * time.Second,
}

func init() {
	rand.Seed(int64(time.Now().Nanosecond()))
}

type Config struct {
	// Endpoints defines a set of URLs (schemes, hosts and ports only)
	// that can be used to communicate with a logical etcd cluster. For
	// example, a three-node cluster could be provided like so:
	//
	// 	Endpoints: []string{
	//		"http://node1.example.com:4001",
	//		"http://node2.example.com:2379",
	//		"http://node3.example.com:4001",
	//	}
	//
	// If multiple endpoints are provided, the Client will attempt to
	// use them all in the event that one or more of them are unusable.
	//
	// If Client.Sync is ever called, the Client may cache an alternate
	// set of endpoints to continue operation.
	Endpoints []string

	// Transport is used by the Client to drive HTTP requests. If not
	// provided, DefaultTransport will be used.
	Transport CancelableTransport

	// CheckRedirect specifies the policy for handling HTTP redirects.
	// If CheckRedirect is not nil, the Client calls it before
	// following an HTTP redirect. The sole argument is the number of
	// requests that have alrady been made. If CheckRedirect returns
	// an error, Client.Do will not make any further requests and return
	// the error back it to the caller.
	//
	// If CheckRedirect is nil, the Client uses its default policy,
	// which is to stop after 10 consecutive requests.
	CheckRedirect CheckRedirectFunc

	// NoAutosync defines whether the client attempts to synchronise
	// with the cluster in regular intervals defined by AutosyncInterval.
	NoAutosync bool

	// AutosyncInterval specifies the time between automatic synchronisation.
	//
	// If zero the DefaultAutosyncInterval is used.
	AutosyncInterval time.Duration
}

func (cfg *Config) transport() CancelableTransport {
	if cfg.Transport == nil {
		return DefaultTransport
	}
	return cfg.Transport
}

func (cfg *Config) checkRedirect() CheckRedirectFunc {
	if cfg.CheckRedirect == nil {
		return DefaultCheckRedirect
	}
	return cfg.CheckRedirect
}

func (cfg *Config) autosyncInterval() time.Duration {
	if cfg.AutosyncInterval == 0 {
		return DefaultAutosyncInterval
	}
	return cfg.AutosyncInterval
}

// CancelableTransport mimics net/http.Transport, but requires that
// the object also support request cancellation.
type CancelableTransport interface {
	http.RoundTripper
	CancelRequest(req *http.Request)
}

type CheckRedirectFunc func(via int) error

// DefaultCheckRedirect follows up to 10 redirects, but no more.
var DefaultCheckRedirect CheckRedirectFunc = func(via int) error {
	if via > 10 {
		return ErrTooManyRedirects
	}
	return nil
}

type Client interface {
	// Sync updates the internal cache of the etcd cluster's membership.
	Sync(context.Context) error

	// Close terminates automatic background synchronisation if running.
	Close()

	// Endpoints returns a copy of the current set of API endpoints used
	// by Client to resolve HTTP requests. If Sync has ever been called,
	// this may differ from the initial Endpoints provided in the Config.
	Endpoints() []string

	httpClient
}

func New(cfg Config) (Client, error) {
	c := &httpClusterClient{
		clientFactory: newHTTPClientFactory(cfg.transport(), cfg.checkRedirect()),
		pinned:        rand.Intn(len(cfg.Endpoints)),
	}
	if err := c.reset(cfg.Endpoints); err != nil {
		return nil, err
	}
	if !cfg.NoAutosync {
		go func() {
			err := c.autosync(cfg.autosyncInterval())
			if err != nil && err != context.Canceled {
				log.Println("client: auto synchronisation failed:", err)
			}
		}()
	}
	c.ctx, c.cancel = context.WithCancel(context.Background())
	return c, nil
}

type httpClient interface {
	Do(context.Context, httpAction) (*http.Response, []byte, error)
}

func newHTTPClientFactory(tr CancelableTransport, cr CheckRedirectFunc) httpClientFactory {
	return func(ep url.URL) httpClient {
		return &redirectFollowingHTTPClient{
			checkRedirect: cr,
			client: &simpleHTTPClient{
				transport: tr,
				endpoint:  ep,
			},
		}
	}
}

type httpClientFactory func(url.URL) httpClient

type httpAction interface {
	HTTPRequest(url.URL) *http.Request
}

type httpClusterClient struct {
	clientFactory httpClientFactory
	endpoints     []url.URL
	pinned        int

	// Context used for automatic synchronisation and its cancel function
	ctx    context.Context
	cancel context.CancelFunc

	sync.RWMutex
}

func (c *httpClusterClient) reset(eps []string) error {
	if len(eps) == 0 {
		return ErrNoEndpoints
	}

	neps := make([]url.URL, len(eps))
	for i, ep := range eps {
		u, err := url.Parse(ep)
		if err != nil {
			return err
		}
		neps[i] = *u
	}

	// keep pinned endpoint if possible
	if len(c.endpoints) > 0 {
		prevPinned := c.endpoints[c.pinned]
		for i, ep := range neps {
			if ep == prevPinned {
				c.pinned = i
				break
			}
		}
	}
	c.endpoints = neps

	return nil
}

func (c *httpClusterClient) Close() {
	if c.cancel != nil {
		c.cancel()
	}
}

func (c *httpClusterClient) Do(ctx context.Context, act httpAction) (*http.Response, []byte, error) {
	c.RLock()
	pin := c.pinned
	leps := len(c.endpoints)
	eps := make([]url.URL, leps)
	copy(eps, c.endpoints)
	c.RUnlock()

	if leps == 0 {
		return nil, nil, ErrNoEndpoints
	}

	hc := c.clientFactory(eps[pin])
	resp, body, err := hc.Do(ctx, act)
	if err == context.DeadlineExceeded || err == context.Canceled {
		return nil, nil, err
	}
	if err == nil && resp.StatusCode/100 != 5 {
		return resp, body, err
	}

	failed := 1

	// synchronise after finishing the request with a new healthy endpoint
	if c.ctx != nil {
		defer func() {
			// break infinite recursion if all endpoints are unhealthy
			if failed == leps {
				return
			}
			ctx, cancel := context.WithTimeout(c.ctx, DefaultRequestTimeout)
			err := c.Sync(ctx)
			if err != nil {
				log.Println("client: error on synchronisation:", err)
			}
			cancel()
		}()
	}

	// try available endpoints randomly and pin the first healthy one
	for _, npin := range rand.Perm(leps) {
		if npin == pin {
			continue
		}
		hc = c.clientFactory(eps[npin])
		resp, body, err = hc.Do(ctx, act)
		if err == context.DeadlineExceeded || err == context.Canceled {
			return nil, nil, err
		}
		if err == nil && resp.StatusCode/100 != 5 {
			// if we had success with a new endpoint update pinned
			c.Lock()
			c.pinned = npin
			c.Unlock()
			return resp, body, err
		}
		failed++
	}
	return nil, nil, err
}

func (c *httpClusterClient) Endpoints() []string {
	c.RLock()
	defer c.RUnlock()

	eps := make([]string, len(c.endpoints))
	for i, ep := range c.endpoints {
		eps[i] = ep.String()
	}

	return eps
}

func (c *httpClusterClient) Sync(ctx context.Context) error {
	mAPI := NewMembersAPI(c)
	ms, err := mAPI.List(ctx)
	if err != nil {
		return err
	}

	c.Lock()
	defer c.Unlock()

	eps := make([]string, 0)
	for _, m := range ms {
		eps = append(eps, m.ClientURLs...)
	}

	return c.reset(eps)
}

func (c *httpClusterClient) autosync(interval time.Duration) error {
	tick := time.NewTicker(interval).C
	for {
		ctx, cancel := context.WithTimeout(c.ctx, DefaultRequestTimeout)
		err := c.Sync(ctx)
		cancel()
		if err != nil && err != context.Canceled {
			log.Println("client: error on auto synchronisation:", err)
		}
		select {
		case <-c.ctx.Done():
			return c.ctx.Err()
		case <-tick:
		}
	}
	return nil
}

type roundTripResponse struct {
	resp *http.Response
	err  error
}

type simpleHTTPClient struct {
	transport CancelableTransport
	endpoint  url.URL
}

func (c *simpleHTTPClient) Do(ctx context.Context, act httpAction) (*http.Response, []byte, error) {
	req := act.HTTPRequest(c.endpoint)

	rtchan := make(chan roundTripResponse, 1)
	go func() {
		resp, err := c.transport.RoundTrip(req)
		rtchan <- roundTripResponse{resp: resp, err: err}
		close(rtchan)
	}()

	var resp *http.Response
	var err error

	select {
	case rtresp := <-rtchan:
		resp, err = rtresp.resp, rtresp.err
	case <-ctx.Done():
		// cancel and wait for request to actually exit before continuing
		c.transport.CancelRequest(req)
		rtresp := <-rtchan
		resp = rtresp.resp
		err = ctx.Err()
	}

	// always check for resp nil-ness to deal with possible
	// race conditions between channels above
	defer func() {
		if resp != nil {
			resp.Body.Close()
		}
	}()

	if err != nil {
		return nil, nil, err
	}

	body, err := ioutil.ReadAll(resp.Body)
	return resp, body, err
}

type redirectFollowingHTTPClient struct {
	client        httpClient
	checkRedirect CheckRedirectFunc
}

func (r *redirectFollowingHTTPClient) Do(ctx context.Context, act httpAction) (*http.Response, []byte, error) {
	next := act
	for i := 0; i < 100; i++ {
		if i > 0 {
			if err := r.checkRedirect(i); err != nil {
				return nil, nil, err
			}
		}
		resp, body, err := r.client.Do(ctx, next)
		if err != nil {
			return nil, nil, err
		}
		if resp.StatusCode/100 == 3 {
			hdr := resp.Header.Get("Location")
			if hdr == "" {
				return nil, nil, fmt.Errorf("Location header not set")
			}
			loc, err := url.Parse(hdr)
			if err != nil {
				return nil, nil, fmt.Errorf("Location header not valid URL: %s", hdr)
			}
			next = &redirectedHTTPAction{
				action:   act,
				location: *loc,
			}
			continue
		}
		return resp, body, nil
	}

	return nil, nil, errTooManyRedirectChecks
}

type redirectedHTTPAction struct {
	action   httpAction
	location url.URL
}

func (r *redirectedHTTPAction) HTTPRequest(ep url.URL) *http.Request {
	orig := r.action.HTTPRequest(ep)
	orig.URL = &r.location
	return orig
}
