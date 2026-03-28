package hkvc

import (
	"encoding/json"
	"net/http"
)

func sendJSONResponse(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}

func (p *HKVCParticipant) handleList(w http.ResponseWriter, r *http.Request) {

}

func (p *HKVCParticipant) handleGetMetadata(w http.ResponseWriter, r *http.Request) {

}

func (p *HKVCParticipant) handleGet(w http.ResponseWriter, r *http.Request) {

}

func (p *HKVCParticipant) handleSet(w http.ResponseWriter, r *http.Request) {

}

func (p *HKVCParticipant) handleCreate(w http.ResponseWriter, r *http.Request) {

}

func (p *HKVCParticipant) handleDelete(w http.ResponseWriter, r *http.Request) {

}
