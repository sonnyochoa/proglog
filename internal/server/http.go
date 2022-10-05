package server

import (
	"encoding/json"
	"net/http"

	"github.com/gorilla/mux"
)

func NewHTTPServer(addr string) *http.Server {
	httpsrv := newHTTPServer()
	r := mux.NewRouter()
	r.HandleFunc("/", httpsrv.handleProduce).Methods("POST")
	r.HandleFunc("/", httpsrv.handleConsume).Methods("GET")
	return &http.Server{
		Addr:    addr,
		Handler: r,
	}
}

type httpServer struct {
	Log *Log
}

// Server referencing a log for the server to defer to its handlers
func newHTTPServer() *httpServer {
	return &httpServer{
		Log: NewLog(),
	}
}

// Contains the record that the caller of our API wants appended to the log
type ProduceRequest struct {
	Record Record `json:"record"`
}

// Tells caller what offset the log store the records under.
type ProduceResponse struct {
	Offset uint64 `json:"offset"`
}

// Which records the caller of our API wants to read.
type ConsumeRequest struct {
	Offset uint64 `json:"offset"`
}

// Send back those records to the caller.
type ConsumeResponse struct {
	Record Record `json:"record"`
}

/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
	When building a json/http Go server, each handler consists of three steps:
	1. Unmarshal the request's JSON body into a struct
 	2. Run the endpoint's logic with the request to obtain a result.
 	3. Marshal and write the result to the response.
* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

// We implement all three steps with the produce handler.
// 1. We unmarshal the request into a struct.
// 2. Use the struct to produce to the log and get the offset that the log stored
// the record under.
// 3. Marshal and write the result to the response.
func (s *httpServer) handleProduce(w http.ResponseWriter, r *http.Request) {
	var req ProduceRequest
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	off, err := s.Log.Append(req.Record)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	res := ProduceResponse{Offset: off}
	err = json.NewEncoder(w).Encode(res)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

// The consume handler is similar to the produce handler, but calls Read(offset uint64)
// to get the record stored in the log. This includes more error checking than produce
// handler. In this method we check to see if the client requested a record that does
// not exist.
func (s *httpServer) handleConsume(w http.ResponseWriter, r *http.Request) {
	var req ConsumeRequest
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	record, err := s.Log.Read(req.Offset)
	if err == ErrOffsetNotFound {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	res := ConsumeResponse{Record: record}
	err = json.NewEncoder(w).Encode(res)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}
