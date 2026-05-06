package poller

import (
	"context"
	"log/slog"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/louisolivier/kost/internal/cache"
	kostmetrics "github.com/louisolivier/kost/internal/metrics"
)

type EC2Client interface {
	DescribeSpotPriceHistory(ctx context.Context, params *ec2.DescribeSpotPriceHistoryInput, optFns ...func(*ec2.Options)) (*ec2.DescribeSpotPriceHistoryOutput, error)
}

type PricingPoller struct {
	client           EC2Client
	instanceType     string
	availabilityZone string
	cache            *cache.MetricsCache
	interval         time.Duration
	logger           *slog.Logger
}

func NewPricingPoller(client EC2Client, instanceType, az string, c *cache.MetricsCache, interval time.Duration, logger *slog.Logger) *PricingPoller {
	return &PricingPoller{
		client: client, instanceType: instanceType, availabilityZone: az,
		cache: c, interval: interval, logger: logger,
	}
}

func (p *PricingPoller) Run(ctx context.Context) {
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

func (p *PricingPoller) poll(ctx context.Context) {
	out, err := p.client.DescribeSpotPriceHistory(ctx, &ec2.DescribeSpotPriceHistoryInput{
		InstanceTypes:       []ec2types.InstanceType{ec2types.InstanceType(p.instanceType)},
		AvailabilityZone:    aws.String(p.availabilityZone),
		ProductDescriptions: []string{"Linux/UNIX"},
		MaxResults:          aws.Int32(1),
	})
	if err != nil {
		p.logger.Error("pricing poll failed", "error", err)
		kostmetrics.PricingPollErrors.With(prometheus.Labels{"scaler": "global"}).Inc()
		return
	}
	if len(out.SpotPriceHistory) == 0 {
		p.logger.Warn("no spot price history returned", "instanceType", p.instanceType, "az", p.availabilityZone)
		return
	}
	price, err := strconv.ParseFloat(aws.ToString(out.SpotPriceHistory[0].SpotPrice), 64)
	if err != nil {
		p.logger.Error("failed to parse spot price", "raw", aws.ToString(out.SpotPriceHistory[0].SpotPrice), "error", err)
		return
	}
	p.cache.SetSpotPrice(price)
}
