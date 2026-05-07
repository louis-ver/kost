package nodeagent_test

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/louisolivier/kost/internal/nodeagent"
)

func TestIMDSPoller_DetectsInterruption(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	poller := nodeagent.NewIMDSPollerWithURL(server.URL, 10*time.Millisecond, slog.Default())
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	if !poller.Run(ctx) {
		t.Fatal("expected interruption to be detected on 200 response")
	}
}

func TestIMDSPoller_NoInterruption_OnNotFound(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

	poller := nodeagent.NewIMDSPollerWithURL(server.URL, 10*time.Millisecond, slog.Default())
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	if poller.Run(ctx) {
		t.Fatal("expected no interruption on 404 response")
	}
}

func TestIMDSPoller_ConnectionError_DoesNotTrigger(t *testing.T) {
	poller := nodeagent.NewIMDSPollerWithURL("http://127.0.0.1:1", 10*time.Millisecond, slog.Default())
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	if poller.Run(ctx) {
		t.Fatal("connection error should not trigger interruption")
	}
}

func TestIMDSPoller_UnexpectedStatus_DoesNotTrigger(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	poller := nodeagent.NewIMDSPollerWithURL(server.URL, 10*time.Millisecond, slog.Default())
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	if poller.Run(ctx) {
		t.Fatal("unexpected status code should not trigger interruption")
	}
}
