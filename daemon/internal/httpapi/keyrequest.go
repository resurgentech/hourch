package httpapi

import (
	"net/http"

	api "github.com/resurgentech/hourch/api"
)

// keyRequest serves GET /v1/key-request (no authentication): where a tool
// without a token files a key request. 404 when personal_keys.request_url is
// not set; a client must not rely on that body.
func (s *Server) keyRequest(w http.ResponseWriter, _ *http.Request, _ caller) {
	if s.o.KeyRequestURL == "" {
		notFound(w, "no key request URL is configured")
		return
	}
	writeJSON(w, http.StatusOK, api.KeyRequestDiscovery{RequestUrl: s.o.KeyRequestURL})
}
