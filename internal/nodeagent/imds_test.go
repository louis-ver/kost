package nodeagent_test

import (
	"context"
	"fmt"
	"log/slog"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/feature/ec2/imds"

	"github.com/louisolivier/kost/internal/nodeagent"
)

// fakeIMDSClient implements IMDSClient for testing.
type fakeIMDSClient struct {
	err error
}

func (f *fakeIMDSClient) GetMetadata(_ context.Context, _ *imds.GetMetadataInput, _ ...func(*imds.Options)) (*imds.GetMetadataOutput, error) {
	return nil, f.err
}

// httpError simulates an AWS SDK error with an HTTP status code.
type httpError struct {
	statusCode int
	msg        string
}

func (e *httpError) Error() string       { return e.msg }
func (e *httpError) HTTPStatusCode() int { return e.statusCode }

func TestIMDSPoller_DetectsInterruption(t *testing.T) {
	// nil error means GetMetadata succeeded = termination-time exists = interruption
	poller := nodeagent.NewIMDSPollerWithClient(&fakeIMDSClient{err: nil}, 10*time.Millisecond, slog.Default())
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	if !poller.Run(ctx) {
		t.Fatal("expected interruption to be detected when GetMetadata returns nil error")
	}
}

func TestIMDSPoller_NoInterruption_OnNotFound(t *testing.T) {
	// 404 error = path not found = no interruption notice yet
	notFound := &httpError{statusCode: 404, msg: "404 Not Found"}
	poller := nodeagent.NewIMDSPollerWithClient(&fakeIMDSClient{err: notFound}, 10*time.Millisecond, slog.Default())
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	if poller.Run(ctx) {
		t.Fatal("expected no interruption on 404 response")
	}
}

func TestIMDSPoller_ConnectionError_DoesNotTrigger(t *testing.T) {
	networkErr := fmt.Errorf("connection refused")
	poller := nodeagent.NewIMDSPollerWithClient(&fakeIMDSClient{err: networkErr}, 10*time.Millisecond, slog.Default())
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	if poller.Run(ctx) {
		t.Fatal("connection error should not trigger interruption")
	}
}

func TestIMDSPoller_UnexpectedStatus_DoesNotTrigger(t *testing.T) {
	serverErr := &httpError{statusCode: 500, msg: "500 Internal Server Error"}
	poller := nodeagent.NewIMDSPollerWithClient(&fakeIMDSClient{err: serverErr}, 10*time.Millisecond, slog.Default())
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	if poller.Run(ctx) {
		t.Fatal("unexpected status code should not trigger interruption")
	}
}
