package monitor

import (
	"strings"
	"testing"
	"time"

	"trendyol/internal/store"
)

func TestEnrichAndRenderMillionProductDashboard(t *testing.T) {
	status := store.Counts{
		Queues: map[string]map[string]int64{
			"direct":      {"pending": 20_000, "processing": 128, "retry": 30, "done": 500, "skip": 5},
			"cloudbypass": {"pending": 0},
		},
		Tables: map[string]int64{"candidate_sources": 50_000},
		HTTP:   map[string]int64{"403": 3, "429": 2, "503": 1, "404": 4},
		Goal:   store.GoalSummary{QualifiedProducts: 500, VariantRows: 1_912},
		Traffic: store.TrafficSummary{
			TotalResponseBytes:     125_000_000,
			TotalResponseGB:        0.125,
			LastMinuteGB:           0.025,
			AverageBytesPerProduct: 250_000,
			ProjectionProducts:     500,
		},
		Ingest: store.IngestSummary{
			ProductsPerMinute:          100,
			VariantsPerMinute:          380,
			AttemptsPerMinute:          120,
			CandidatesPerMinute:        2_000,
			SitemapCandidatesPerMinute: 1_800,
			PublicCandidatesPerMinute:  200,
			SuccessesPerMinute:         100,
			SuccessRatePct:             83.3,
			CumulativeSuccesses:        500,
			CumulativeAttempts:         800,
			CumulativeSuccessRatePct:   62.5,
		},
		Minute:    map[string]float64{"done": 100, "retry": 15, "skip": 5},
		Quality:   map[string]float64{"title_pct": 100, "main_image_pct": 100, "category_pct": 100, "variation_complete_pct": 99.5},
		Discovery: map[string]string{"priority_high": "20000", "discovery_sitemap_index": "12", "discovery_sitemap_offset": "3456"},
		Backfill:  store.BackfillSummary{TotalProducts: 60_021, FilledProducts: 12_000, ActiveJobs: 47_021, TerminalMissing: 1_000},
	}
	Enrich(&status, 1_000_000)
	if status.Traffic.ProjectedTargetGB != 250 || status.Traffic.ProjectedRemainingGB != 249.875 {
		t.Fatalf("traffic=%+v", status.Traffic)
	}
	if status.Ingest.ETASeconds <= 0 || status.Goal.ProgressPct != 0.05 {
		t.Fatalf("goal=%+v ingest=%+v", status.Goal, status.Ingest)
	}
	rendered := Render(status, Options{ProductTarget: 1_000_000, PendingLow: 20_000, PendingTarget: 50_000, FreshPendingLow: 5_000, FreshPendingTarget: 20_000, ConfiguredProxies: 1, ProxyBudgetGB: 100, ProxyStopReserveGB: 2}, time.Date(2026, 8, 23, 12, 0, 0, 0, time.UTC))
	for _, expected := range []string{"1,000,000", "当前一分钟 0.025000 GB", "套餐额度 100.000 GB", "剩余 99.875 GB", "停机线 98.000 GB", "旧数据补齐（新商品已暂停）", "已补齐 12,000 / 60,021", "补齐成功率 92.3%", "抓取成功率 最近60秒 83.3%（100/120）", "累计 62.5%（500/800）", "预估100万商品 250.00 GB", "商品 100/分钟", "Sitemap 1,800", "CloudBypass=关闭"} {
		if !strings.Contains(rendered, expected) {
			t.Fatalf("dashboard missing %q:\n%s", expected, rendered)
		}
	}
}
