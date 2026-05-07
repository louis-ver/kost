package nodeagent

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/aws/aws-sdk-go-v2/feature/ec2/imds"
)

// IMDSClient is an interface over the AWS IMDS client for testability.
type IMDSClient interface {
	GetMetadata(ctx context.Context, params *imds.GetMetadataInput, optFns ...func(*imds.Options)) (*imds.GetMetadataOutput, error)
}

type IMDSPoller struct {
	client   IMDSClient
	interval time.Duration
	logger   *slog.Logger
}

// NewIMDSPoller creates a poller using the real AWS IMDS client (handles IMDSv1 and IMDSv2).
func NewIMDSPoller(interval time.Duration, logger *slog.Logger) *IMDSPoller {
	return &IMDSPoller{
		client:   imds.New(imds.Options{}),
		interval: interval,
		logger:   logger,
	}
}

// NewIMDSPollerWithClient creates a poller with an injectable IMDS client for testing.
func NewIMDSPollerWithClient(client IMDSClient, interval time.Duration, logger *slog.Logger) *IMDSPoller {
	return &IMDSPoller{client: client, interval: interval, logger: logger}
}

// Run polls IMDS until an interruption notice is detected or ctx is cancelled.
// Returns true if an interruption notice was received.
func (p *IMDSPoller) Run(ctx context.Context) bool {
	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return false
		case <-ticker.C:
			if p.poll(ctx) {
				return true
			}
		}
	}
}

func (p *IMDSPoller) poll(ctx context.Context) bool {
	_, err := p.client.GetMetadata(ctx, &imds.GetMetadataInput{
		Path: "spot/termination-time",
	})
	if err == nil {
		// Successful response means the termination-time field exists — interruption notice received
		p.logger.Info("imds: spot interruption notice received")
		return true
	}

	// Check if the error is a 404-equivalent (path not found = no interruption notice)
	var notFoundErr interface{ HTTPStatusCode() int }
	if errors.As(err, &notFoundErr) && notFoundErr.HTTPStatusCode() == 404 {
		return false // no notice yet
	}

	// Any other error (network, IMDSv2 token failure, etc.) — log and keep polling
	p.logger.Warn("imds: poll failed", "error", err)
	return false
}
