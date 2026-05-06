package poller_test

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/louisolivier/kost/internal/cache"
	"github.com/louisolivier/kost/internal/poller"
)

type fakeSQSClient struct {
	queued   string
	inFlight string
	err      error
}

func (f *fakeSQSClient) GetQueueAttributes(_ context.Context, _ *sqs.GetQueueAttributesInput, _ ...func(*sqs.Options)) (*sqs.GetQueueAttributesOutput, error) {
	if f.err != nil {
		return nil, f.err
	}
	return &sqs.GetQueueAttributesOutput{
		Attributes: map[string]string{
			"ApproximateNumberOfMessages":           f.queued,
			"ApproximateNumberOfMessagesNotVisible": f.inFlight,
		},
	}, nil
}

func TestSQSPoller_UpdatesCache(t *testing.T) {
	c := cache.New()
	p := poller.NewSQSPoller(&fakeSQSClient{queued: "70", inFlight: "30"}, "https://sqs.fake/q", c, 50*time.Millisecond, slog.Default())

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	go p.Run(ctx)

	time.Sleep(150 * time.Millisecond)

	depth, _, ok := c.QueueDepth()
	if !ok {
		t.Fatal("expected cache to be populated")
	}
	if depth != 100 {
		t.Errorf("expected 100 (70+30), got %d", depth)
	}
}

func TestSQSPoller_ErrorLeavesCache(t *testing.T) {
	c := cache.New()
	c.SetQueueDepth(50)
	p := poller.NewSQSPoller(&fakeSQSClient{err: errors.New("connection refused")}, "https://sqs.fake/q", c, 50*time.Millisecond, slog.Default())

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	go p.Run(ctx)

	time.Sleep(150 * time.Millisecond)

	depth, _, ok := c.QueueDepth()
	if !ok {
		t.Fatal("expected cache to still have pre-populated data")
	}
	if depth != 50 {
		t.Errorf("expected depth unchanged at 50, got %d", depth)
	}
}
