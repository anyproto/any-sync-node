package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/anytypeio/any-sync-node/nodehead"
	"github.com/anytypeio/any-sync-node/nodespace/peermanager"
	"github.com/anytypeio/any-sync-node/nodesync"
	"github.com/anytypeio/any-sync-node/nodesync/coldsync"
	"github.com/anytypeio/any-sync-node/nodesync/hotsync"
	"github.com/anytypeio/any-sync/coordinator/coordinatorclient"
	"github.com/anytypeio/any-sync/coordinator/nodeconfsource"
	"github.com/anytypeio/any-sync/net/dialer"
	"github.com/anytypeio/any-sync/net/pool"
	"github.com/anytypeio/any-sync/net/streampool"
	"github.com/anytypeio/any-sync/nodeconf"
	"github.com/anytypeio/any-sync/nodeconf/nodeconfstore"

	// import this to keep govvv in go.mod on mod tidy
	_ "github.com/ahmetb/govvv/integration-test/app-different-package/mypkg"
	"github.com/anytypeio/any-sync-node/account"
	"github.com/anytypeio/any-sync-node/config"
	"github.com/anytypeio/any-sync-node/debug/nodedebugrpc"
	"github.com/anytypeio/any-sync-node/nodespace"
	"github.com/anytypeio/any-sync-node/nodespace/nodecache"
	"github.com/anytypeio/any-sync-node/nodestorage"
	"github.com/anytypeio/any-sync/app"
	"github.com/anytypeio/any-sync/app/logger"
	"github.com/anytypeio/any-sync/commonspace"
	"github.com/anytypeio/any-sync/metric"
	"github.com/anytypeio/any-sync/net/rpc/server"
	"github.com/anytypeio/any-sync/net/secureservice"
	"go.uber.org/zap"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"syscall"
	"time"
)

var log = logger.NewNamed("main")

var (
	flagConfigFile = flag.String("c", "etc/any-sync-node.yml", "path to config file")
	flagVersion    = flag.Bool("v", false, "show version and exit")
	flagHelp       = flag.Bool("h", false, "show help and exit")
)

func main() {
	flag.Parse()

	if *flagVersion {
		fmt.Println(app.VersionDescription())
		return
	}
	if *flagHelp {
		flag.PrintDefaults()
		return
	}

	if debug, ok := os.LookupEnv("ANYPROF"); ok && debug != "" {
		go func() {
			http.ListenAndServe(debug, nil)
		}()
	}

	// create app
	ctx := context.Background()
	a := new(app.App)

	// open config file
	conf, err := config.NewFromFile(*flagConfigFile)
	if err != nil {
		log.Fatal("can't open config file", zap.Error(err))
	}

	// bootstrap components
	a.Register(conf)
	Bootstrap(a)

	// start app
	if err := a.Start(ctx); err != nil {
		log.Fatal("can't start app", zap.Error(err))
	}
	log.Info("app started", zap.String("version", a.Version()))

	// wait exit signal
	exit := make(chan os.Signal, 1)
	signal.Notify(exit, os.Interrupt, syscall.SIGKILL, syscall.SIGTERM, syscall.SIGQUIT)
	sig := <-exit
	log.Info("received exit signal, stop app...", zap.String("signal", fmt.Sprint(sig)))

	// close app
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	if err := a.Close(ctx); err != nil {
		log.Fatal("close error", zap.Error(err))
	} else {
		log.Info("goodbye!")
	}
	time.Sleep(time.Second / 3)
}

func Bootstrap(a *app.App) {
	a.Register(account.New()).
		Register(coordinatorclient.New()).
		Register(nodeconfstore.New()).
		Register(nodeconfsource.New()).
		Register(nodeconf.New()).
		Register(dialer.New()).
		Register(pool.New()).
		Register(metric.New()).
		Register(nodehead.New()).
		Register(nodestorage.New()).
		Register(nodecache.New(200)).
		Register(hotsync.New()).
		Register(coldsync.New()).
		Register(nodesync.New()).
		Register(secureservice.New()).
		Register(nodespace.New()).
		Register(commonspace.New()).
		Register(streampool.New()).
		Register(peermanager.New()).
		Register(server.New()).
		Register(nodedebugrpc.New())
}
