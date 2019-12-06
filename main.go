package main

import (
	"fmt"
	"io"
	stdlog "log"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"time"

	"github.com/dgraph-io/badger"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
	"github.com/rs/zerolog"
)

func main() {
	logger := zerolog.New(os.Stdout)

	rawConfig := readRawConfig()
	config, err := resolveConfig(rawConfig)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Configuration errors - %s\n", err)
		os.Exit(1)
	}

	nodeLogger := logger.With().Str("component", "node").Logger()
	fmt.Println("\n.........Here.........\n")
	node, err := NewNode(config, &nodeLogger)
	fmt.Println("\n.........Done.........\n")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error configuring node: %s", err)
		os.Exit(1)
	}

	if config.JoinAddress != "" {
		go func() {
			retryJoin := func() error {
				url := url.URL{
					Scheme: "http",
					Host:   config.JoinAddress,
					Path:   "join",
				}

				req, err := http.NewRequest(http.MethodPost, url.String(), nil)
				if err != nil {
					return err
				}
				req.Header.Add("Peer-Address", config.RaftAddress.String())

				resp, err := http.DefaultClient.Do(req)
				if err != nil {
					return err
				}

				if resp.StatusCode != http.StatusOK {
					return fmt.Errorf("non 200 status code: %d", resp.StatusCode)
				}

				return nil
			}

			for {
				if err := retryJoin(); err != nil {
					logger.Error().Err(err).Str("component", "join").Msg("Error joining cluster")
					time.Sleep(1 * time.Second)
				} else {
					break
				}
			}
		}()
	}

	httpLogger := logger.With().Str("component", "http").Logger()
	httpServer := &httpServer{
		node:    node,
		address: config.HTTPAddress,
		logger:  &httpLogger,
	}

	httpServer.Start()

}

type node struct {
	config   *Config
	raftNode *raft.Raft
	fsm      *fsm
	log      *zerolog.Logger
}

func NewNode(config *Config, log *zerolog.Logger) (*node, error) {
	dirOffset := fmt.Sprintf("%v", time.Now().Unix())
	fmt.Println("Dir: ", dirOffset)
	database, err := badger.Open(badger.DefaultOptions("/tmp/badger" + dirOffset))
	fmt.Println("\n\n......OPENED..........\n\n")
	if err != nil {
		fmt.Println("Error in db creation")
		panic(err)
	}
	// defer database.Close()
	// var mut sync.Mutex
	fsm := &fsm{
		db: database,
	}
	fmt.Println("\n\n......FSM CREATED..........\n\n")

	if err := os.MkdirAll(config.DataDir, 0700); err != nil {
		return nil, err
	}
	fmt.Println("\n\n......DIRECTORY OPENED..........\n\n")

	raftConfig := raft.DefaultConfig()
	raftConfig.LocalID = raft.ServerID(config.RaftAddress.String())
	raftConfig.Logger = stdlog.New(log, "", 0)
	transportLogger := log.With().Str("component", "raft-transport").Logger()
	transport, err := raftTransport(config.RaftAddress, transportLogger)
	if err != nil {
		return nil, err
	}
	fmt.Println("\n\n......1..........\n\n")

	snapshotStoreLogger := log.With().Str("component", "raft-snapshots").Logger()
	snapshotStore, err := raft.NewFileSnapshotStore(config.DataDir+dirOffset, 1, snapshotStoreLogger)
	if err != nil {
		return nil, err
	}
	fmt.Println("\n\n......2..........\n\n")

	logStore, err := raftboltdb.NewBoltStore(filepath.Join(config.DataDir+dirOffset, "raft-log.bolt"))
	if err != nil {
		return nil, err
	}
	fmt.Println("\n\n......3..........\n\n")

	stableStore, err := raftboltdb.NewBoltStore(filepath.Join(config.DataDir+dirOffset, "raft-stable.bolt"))
	if err != nil {
		return nil, err
	}
	fmt.Println("\n\n......4..........\n\n")

	raftNode, err := raft.NewRaft(raftConfig, fsm, logStore, stableStore,
		snapshotStore, transport)
	if err != nil {
		return nil, err
	}
	fmt.Println("\n\n......5..........\n\n")
	if config.Bootstrap {
		fmt.Println("\n\n......5.1..........\n\n")
		configuration := raft.Configuration{
			Servers: []raft.Server{
				{
					ID:      raftConfig.LocalID,
					Address: transport.LocalAddr(),
				},
			},
		}
		raftNode.BootstrapCluster(configuration)
	}
	fmt.Println("\n\n......6..........\n\n")
	return &node{
		config:   config,
		raftNode: raftNode,
		log:      log,
		fsm:      fsm,
	}, nil
}

func raftTransport(raftAddr net.Addr, log io.Writer) (*raft.NetworkTransport, error) {
	address, err := net.ResolveTCPAddr("tcp", raftAddr.String())
	if err != nil {
		return nil, err
	}

	transport, err := raft.NewTCPTransport(address.String(), address, 3, 10*time.Second, log)
	if err != nil {
		return nil, err
	}

	return transport, nil
}
