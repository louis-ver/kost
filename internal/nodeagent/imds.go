package nodeagent

import (
	"context"
	"log/slog"
	"net/http"
	"time"
)

const imdsTerminationURL = "http://169.254.169.254/latest/meta-data/spot/termination-time"

type IMDSPoller struct {
	url      string
	client   *http.Client
	interval time.Duration
	logger   *slog.Logger
}

func NewIMDSPoller(interval time.Duration, logger *slog.Logger) *IMDSPoller {
	return NewIMDSPollerWithURL(imdsTerminationURL, interval, logger)
}

func NewIMDSPollerWithURL(url string, interval time.Duration, logger *slog.Logger) *IMDSPoller {
	return &IMDSPoller{
		url:      url,
		client:   &http.Client{Timeout: time.Second},
		interval: interval,
		logger:   logger,
	}
}

// Run polls IMDS until an interruption notice is detected or ctx is cancelled.
// Returns true if an interruption notice was received (HTTP 200).
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
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, p.url, nil)
	if err != nil {
		p.logger.Warn("imds: failed to build request", "error", err)
		return false
	}
	resp, err := p.client.Do(req)
	if err != nil {
		p.logger.Warn("imds: poll failed", "error", err)
		return false
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusOK:
		p.logger.Info("imds: spot interruption notice received")
		return true
	case http.StatusNotFound:
		return false
	default:
		p.logger.Warn("imds: unexpected status", "status", resp.StatusCode)
		return false
	}
}
