package poller

import (
	"context"
	"log/slog"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/louisolivier/kost/internal/cache"
)

type SQSClient interface {
	GetQueueAttributes(ctx context.Context, params *sqs.GetQueueAttributesInput, optFns ...func(*sqs.Options)) (*sqs.GetQueueAttributesOutput, error)
}

type SQSPoller struct {
	client   SQSClient
	queueURL string
	cache    *cache.MetricsCache
	interval time.Duration
	logger   *slog.Logger
}

func NewSQSPoller(client SQSClient, queueURL string, c *cache.MetricsCache, interval time.Duration, logger *slog.Logger) *SQSPoller {
	return &SQSPoller{client: client, queueURL: queueURL, cache: c, interval: interval, logger: logger}
}

func (p *SQSPoller) Run(ctx context.Context) {
	p.poll(ctx)
	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			p.poll(ctx)
		}
	}
}

func (p *SQSPoller) poll(ctx context.Context) {
	out, err := p.client.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl: &p.queueURL,
		AttributeNames: []sqstypes.QueueAttributeName{
			sqstypes.QueueAttributeNameApproximateNumberOfMessages,
			sqstypes.QueueAttributeNameApproximateNumberOfMessagesNotVisible,
		},
	})
	if err != nil {
		p.logger.Error("sqs poll failed", "error", err)
		return
	}
	queued, _ := strconv.ParseInt(out.Attributes["ApproximateNumberOfMessages"], 10, 64)
	inFlight, _ := strconv.ParseInt(out.Attributes["ApproximateNumberOfMessagesNotVisible"], 10, 64)
	p.cache.SetQueueDepth(queued + inFlight)
}
