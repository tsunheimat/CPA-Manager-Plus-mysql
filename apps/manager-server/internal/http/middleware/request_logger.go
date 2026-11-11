package middleware

import (
	"context"
	"crypto/rand"
	"log"
	"net/http"
	"time"
)

type requestIDKey struct{}

func RequestID(ctx context.Context) string {
	id, _ := ctx.Value(requestIDKey{}).(string)
	return id
}

// EnsureRequestID also supports handlers mounted without RequestLogger. Never
// trust a client-provided header as a log field or permit it to collide with
// another request's correlation ID.
func EnsureRequestID(r *http.Request) (*http.Request, string) {
	if id := RequestID(r.Context()); id != "" {
		return r, id
	}
	id := rand.Text()
	return r.WithContext(context.WithValue(r.Context(), requestIDKey{}, id)), id
}

func RequestLogger(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started := time.Now()
		r, id := EnsureRequestID(r)
		w.Header().Set("X-Request-ID", id)
		recorder := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(recorder, r)
		log.Printf(
			"http request_id=%s method=%s path=%q status=%d duration=%s remote=%q",
			id, r.Method, r.URL.Path, recorder.status, time.Since(started), r.RemoteAddr,
		)
	})
}

type statusRecorder struct {
	http.ResponseWriter
	status      int
	wroteHeader bool
}

func (r *statusRecorder) WriteHeader(status int) {
	if !r.wroteHeader {
		r.status = status
		r.wroteHeader = status >= 200
	}
	r.ResponseWriter.WriteHeader(status)
}

func (r *statusRecorder) Write(p []byte) (int, error) {
	if !r.wroteHeader {
		r.WriteHeader(http.StatusOK)
	}
	return r.ResponseWriter.Write(p)
}

func (r *statusRecorder) Unwrap() http.ResponseWriter { return r.ResponseWriter }
