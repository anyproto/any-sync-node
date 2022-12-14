package dialer

import (
	"context"
	"errors"
	"github.com/anytypeio/go-anytype-infrastructure-experiments/common/app"
	"github.com/anytypeio/go-anytype-infrastructure-experiments/common/app/logger"
	"github.com/anytypeio/go-anytype-infrastructure-experiments/common/config"
	"github.com/anytypeio/go-anytype-infrastructure-experiments/common/net/peer"
	"github.com/anytypeio/go-anytype-infrastructure-experiments/common/net/secure"
	"github.com/anytypeio/go-anytype-infrastructure-experiments/common/net/timeoutconn"
	"github.com/libp2p/go-libp2p/core/sec"
	"go.uber.org/zap"
	"net"
	"storj.io/drpc"
	"storj.io/drpc/drpcconn"
	"storj.io/drpc/drpcmanager"
	"storj.io/drpc/drpcwire"
	"sync"
	"time"
)

const CName = "common.net.dialer"

var ErrArrdsNotFound = errors.New("addrs for peer not found")

var log = logger.NewNamed(CName)

func New() Dialer {
	return &dialer{}
}

type Dialer interface {
	Dial(ctx context.Context, peerId string) (peer peer.Peer, err error)
	UpdateAddrs(addrs map[string][]string)
	app.Component
}

type dialer struct {
	transport secure.Service
	config    *config.Config
	peerAddrs map[string][]string

	mu sync.RWMutex
}

func (d *dialer) Init(a *app.App) (err error) {
	d.transport = a.MustComponent(secure.CName).(secure.Service)
	d.config = a.MustComponent(config.CName).(*config.Config)
	d.peerAddrs = map[string][]string{}
	for _, n := range d.config.Nodes {
		d.peerAddrs[n.PeerId] = []string{n.Address}
	}
	return
}

func (d *dialer) Name() (name string) {
	return CName
}

func (d *dialer) UpdateAddrs(addrs map[string][]string) {
	d.mu.Lock()
	d.peerAddrs = addrs
	d.mu.Unlock()
}

func (d *dialer) Dial(ctx context.Context, peerId string) (p peer.Peer, err error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	addrs, ok := d.peerAddrs[peerId]
	if !ok || len(addrs) == 0 {
		return nil, ErrArrdsNotFound
	}
	var (
		conn drpc.Conn
		sc   sec.SecureConn
	)
	for _, addr := range addrs {
		conn, sc, err = d.handshake(ctx, addr)
		if err != nil {
			log.Info("can't connect to host", zap.String("addr", addr))
		} else {
			err = nil
			break
		}
	}
	if err != nil {
		return
	}
	return peer.NewPeer(sc, conn), nil
}

func (d *dialer) handshake(ctx context.Context, addr string) (conn drpc.Conn, sc sec.SecureConn, err error) {
	tcpConn, err := net.Dial("tcp", addr)
	if err != nil {
		return
	}

	timeoutConn := timeoutconn.NewConn(tcpConn, time.Millisecond*time.Duration(d.config.Stream.TimeoutMilliseconds))
	sc, err = d.transport.TLSConn(ctx, timeoutConn)
	if err != nil {
		return
	}
	log.Info("connected with remote host", zap.String("serverPeer", sc.RemotePeer().String()), zap.String("per", sc.LocalPeer().String()))
	conn = drpcconn.NewWithOptions(sc, drpcconn.Options{Manager: drpcmanager.Options{
		Reader: drpcwire.ReaderOptions{MaximumBufferSize: d.config.Stream.MaxMsgSizeMb * (1 << 20)},
	}})
	return conn, sc, err
}
