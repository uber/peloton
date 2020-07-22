package detector

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io/ioutil"
	"net"
	"strconv"
	"strings"
	"sync"

	"github.com/golang/protobuf/proto"
	log "github.com/sirupsen/logrus"
	mesos "github.com/uber/peloton/.gen/mesos/v1"
	util "github.com/uber/peloton/pkg/hostmgr/mesos/mesos-go/mesosutil"
	"github.com/uber/peloton/pkg/hostmgr/mesos/mesos-go/upid"
)

var (
	pluginLock sync.Mutex
	plugins    = map[string]PluginFactory{}
	// ErrEmptySpec is the error for when no main is provided
	ErrEmptySpec = errors.New("empty main specification")

	defaultFactory = PluginFactory(func(spec string, _ ...Option) (Main, error) {
		if len(spec) == 0 {
			return nil, ErrEmptySpec
		}
		if strings.Index(spec, "@") < 0 {
			spec = "main@" + spec
		}
		if pid, err := upid.Parse(spec); err == nil {
			return NewStandalone(CreateMainInfo(pid)), nil
		} else {
			return nil, err
		}
	})
)

type PluginFactory func(string, ...Option) (Main, error)

// associates a plugin implementation with a Main specification prefix.
// packages that provide plugins are expected to invoke this func within
// their init() implementation. schedulers that wish to support plugins may
// anonymously import ("_") a package the auto-registers said plugins.
func Register(prefix string, f PluginFactory) error {
	if prefix == "" {
		return fmt.Errorf("illegal prefix: '%v'", prefix)
	}
	if f == nil {
		return fmt.Errorf("nil plugin factories are not allowed")
	}

	pluginLock.Lock()
	defer pluginLock.Unlock()

	if _, found := plugins[prefix]; found {
		return fmt.Errorf("detection plugin already registered for prefix '%s'", prefix)
	}
	plugins[prefix] = f
	return nil
}

// Create a new detector given the provided specification. Examples are:
//
//   - file://{path_to_local_file}
//   - {ipaddress}:{port}
//   - main@{ip_address}:{port}
//   - main({id})@{ip_address}:{port}
//
// Support for the file:// prefix is intentionally hardcoded so that it may
// not be inadvertently overridden by a custom plugin implementation. Custom
// plugins are supported via the Register and MatchingPlugin funcs.
//
// Furthermore it is expected that main detectors returned from this func
// are not yet running and will only begin to spawn requisite background
// processing upon, or some time after, the first invocation of their Detect.
//
func New(spec string, options ...Option) (m Main, err error) {
	if strings.HasPrefix(spec, "file://") {
		var body []byte
		path := spec[7:]
		body, err = ioutil.ReadFile(path)
		if err != nil {
			log.Infof("failed to read from file at '%s'", path)
		} else {
			m, err = New(string(body), options...)
		}
	} else if f, ok := MatchingPlugin(spec); ok {
		m, err = f(spec, options...)
	} else {
		m, err = defaultFactory(spec, options...)
	}

	return
}

func MatchingPlugin(spec string) (PluginFactory, bool) {
	pluginLock.Lock()
	defer pluginLock.Unlock()

	for prefix, f := range plugins {
		if strings.HasPrefix(spec, prefix) {
			return f, true
		}
	}
	return nil, false
}

// Super-useful utility func that attempts to build a mesos.MainInfo from a
// upid.UPID specification. An attempt is made to determine the IP address of
// the UPID's Host and any errors during such resolution will result in a nil
// returned result. A nil result is also returned upon errors parsing the Port
// specification of the UPID.
//
// TODO(jdef) make this a func of upid.UPID so that callers can invoke somePid.MainInfo()?
//
func CreateMainInfo(pid *upid.UPID) *mesos.MainInfo {
	if pid == nil {
		return nil
	}
	port, err := strconv.Atoi(pid.Port)
	if err != nil {
		log.Errorf("failed to parse port: %v", err)
		return nil
	}
	//TODO(jdef) what about (future) ipv6 support?
	var ipv4 net.IP
	if ipv4 = net.ParseIP(pid.Host); ipv4 != nil {
		// This is needed for the people cross-compiling from macos to linux.
		// The cross-compiled version of net.LookupIP() fails to handle plain IPs.
		// See https://github.com/mesos/mesos-go/pull/117
	} else if addrs, err := net.LookupIP(pid.Host); err == nil {
		for _, ip := range addrs {
			if ip = ip.To4(); ip != nil {
				ipv4 = ip
				break
			}
		}
		if ipv4 == nil {
			log.Errorf("host does not resolve to an IPv4 address: %v", pid.Host)
			return nil
		}
	} else {
		log.Errorf("failed to lookup IPs for host '%v': %v", pid.Host, err)
		return nil
	}
	packedip := binary.BigEndian.Uint32(ipv4) // network byte order is big-endian
	mi := util.NewMainInfo(pid.ID, packedip, uint32(port))
	mi.Pid = proto.String(pid.String())
	if pid.Host != "" {
		mi.Hostname = proto.String(pid.Host)
	}
	return mi
}
