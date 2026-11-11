package middleware

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestRequestLoggerGeneratesCorrelationAndIgnoresClientID(t *testing.T) {
	var observed string
	handler := RequestLogger(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		observed = RequestID(r.Context())
		again, id := EnsureRequestID(r)
		if again != r || id != observed {
			t.Error("nested handler changed correlation")
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	previous := ""
	for range 2 {
		r := httptest.NewRequest(http.MethodGet, "/safe?secret=not-logged", nil)
		r.Header.Set("X-Request-ID", "forged")
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r)
		if observed == "" || observed == "forged" || observed == previous || w.Header().Get("X-Request-ID") != observed {
			t.Fatal("missing server-generated correlation")
		}
		previous = observed
	}
}
