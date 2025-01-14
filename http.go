package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/dgraph-io/badger"
	"github.com/hashicorp/raft"
	"github.com/justinas/alice"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/hlog"
)

type httpServer struct {
	address net.Addr
	node    *node
	logger  *zerolog.Logger
}

func (server *httpServer) Start() {
	server.logger.Info().Str("address", server.address.String()).Msg("Starting server")
	c := alice.New()
	c = c.Append(hlog.NewHandler(*server.logger))
	c = c.Append(hlog.AccessHandler(func(r *http.Request, status, size int, duration time.Duration) {
		hlog.FromRequest(r).Info().
			Str("req.method", r.Method).
			Str("req.url", r.URL.String()).
			Int("req.status", status).
			Int("req.size", size).
			Dur("req.duration", duration).
			Msg("")
	}))
	c = c.Append(hlog.RemoteAddrHandler("req.ip"))
	c = c.Append(hlog.UserAgentHandler("req.useragent"))
	c = c.Append(hlog.RefererHandler("req.referer"))
	c = c.Append(hlog.RequestIDHandler("req.id", "Request-Id"))
	handler := c.Then(server)

	if err := http.ListenAndServe(server.address.String(), handler); err != nil {
		server.logger.Fatal().Err(err).Msg("Error running HTTP server")
	}
}

func (server *httpServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if strings.Contains(r.URL.Path, "/key") {
		server.handleRequest(w, r)
	} else if strings.Contains(r.URL.Path, "/join") {
		server.handleJoin(w, r)
	} else {
		w.WriteHeader(http.StatusBadRequest)
	}
}

func (server *httpServer) handleRequest(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		server.handleKeyPost(w, r)
		fmt.Println("Done serving...")
		return
	case http.MethodGet:
		server.handleKeyGet(w, r)
		return
	}
	w.WriteHeader(http.StatusMethodNotAllowed)
}

func (server *httpServer) handleKeyPost(w http.ResponseWriter, r *http.Request) {
	// request := struct {
	// 	NewValue int `json:"newValue"`
	// }{}

	request := make(map[string]interface{})

	defer r.Body.Close()
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		server.logger.Error().Err(err).Msg("Bad request")
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	fmt.Println(".........Post body: ", request, "...........")

	event := &event{
		Type:  "set",
		Value: request,
	}
	fmt.Println("1")
	eventBytes, err := json.Marshal(event)
	fmt.Println("2")
	if err != nil {
		server.logger.Error().Err(err).Msg("")
		return
	}
	fmt.Println("3")

	//application of raft algorithm for replication
	applyFuture := server.node.raftNode.Apply(eventBytes, 5*time.Second)
	fmt.Println("4")
	if err := applyFuture.Error(); err != nil {
		fmt.Println("4.1")
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	fmt.Println("5")

	w.WriteHeader(http.StatusOK)
}

//access value from db
func getValue(db *badger.DB, key string) string {
	var valcopy []byte
	err := db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(key))
		if err != nil {
			return errors.New("Read None")
		}
		e := item.Value(func(val []byte) error {
			// Copying or parsing val is valid.
			valcopy = append([]byte{}, val...)
			fmt.Println(".......Value: ", valcopy, ".........")
			return nil
		})
		if e != nil {
			return errors.New("Error None")
		}
		return nil
	})
	if err != nil {
		fmt.Println("Hah!")
		return err.Error()
	}
	return string(valcopy)
}

func (server *httpServer) handleKeyGet(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)

	response := struct {
		Value string `json:"value"`
	}{
		//update: get value from database
		// Value: server.node.fsm.stateValue,
		Value: getValue(server.node.fsm.db, "answer"),
	}

	responseBytes, err := json.Marshal(response)
	if err != nil {
		server.logger.Error().Err(err).Msg("")
	}

	w.Write(responseBytes)
}

func (server *httpServer) handleJoin(w http.ResponseWriter, r *http.Request) {
	peerAddress := r.Header.Get("Peer-Address")
	if peerAddress == "" {
		server.logger.Error().Msg("Peer-Address not set on request")
		w.WriteHeader(http.StatusBadRequest)
	}

	addPeerFuture := server.node.raftNode.AddVoter(
		raft.ServerID(peerAddress), raft.ServerAddress(peerAddress), 0, 0)
	if err := addPeerFuture.Error(); err != nil {
		server.logger.Error().
			Err(err).
			Str("peer.remoteaddr", peerAddress).
			Msg("Error joining peer to Raft")
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	server.logger.Info().Str("peer.remoteaddr", peerAddress).Msg("Peer joined Raft")
	w.WriteHeader(http.StatusOK)
}
