package poller_test

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/louisolivier/kost/internal/cache"
	"github.com/louisolivier/kost/internal/poller"
)

type fakeEC2Client struct {
	price string
	err   error
}

func (f *fakeEC2Client) DescribeSpotPriceHistory(_ context.Context, _ *ec2.DescribeSpotPriceHistoryInput, _ ...func(*ec2.Options)) (*ec2.DescribeSpotPriceHistoryOutput, error) {
	if f.err != nil {
		return nil, f.err
	}
	return &ec2.DescribeSpotPriceHistoryOutput{
		SpotPriceHistory: []ec2types.SpotPrice{
			{SpotPrice: aws.String(f.price)},
		},
	}, nil
}

func TestPricingPoller_UpdatesCache(t *testing.T) {
	c := cache.New()
	p := poller.NewPricingPoller(&fakeEC2Client{price: "0.089"}, "m5.xlarge", "us-east-1a", c, 50*time.Millisecond, slog.Default())

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	go p.Run(ctx)

	time.Sleep(150 * time.Millisecond)

	price, _, ok := c.SpotPrice()
	if !ok {
		t.Fatal("expected cache to be populated")
	}
	if price != 0.089 {
		t.Errorf("expected 0.089, got %f", price)
	}
}

func TestPricingPoller_ErrorLeavesCache(t *testing.T) {
	c := cache.New()
	c.SetSpotPrice(0.05)
	p := poller.NewPricingPoller(&fakeEC2Client{err: errors.New("throttled")}, "m5.xlarge", "us-east-1a", c, 50*time.Millisecond, slog.Default())

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	go p.Run(ctx)

	time.Sleep(150 * time.Millisecond)

	price, _, ok := c.SpotPrice()
	if !ok {
		t.Fatal("expected cache to still have data")
	}
	if price != 0.05 {
		t.Errorf("expected 0.05 unchanged, got %f", price)
	}
}
