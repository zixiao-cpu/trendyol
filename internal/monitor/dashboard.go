package monitor

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"trendyol/internal/store"
)

const decimalGB = 1_000_000_000

type Options struct {
	ProductTarget      int64
	PendingLow         int
	PendingTarget      int
	FreshPendingLow    int
	FreshPendingTarget int
	ConfiguredProxies  int64
	CloudBypassEnabled bool
	ProxyBudgetGB      float64
	ProxyStopReserveGB float64
}

// Enrich adds target-dependent projections to a storage snapshot. Traffic is
// projected from every recorded proxy response (successful pages, retries and
// discovery) divided by accepted products, so failures are not hidden.
func Enrich(status *store.Counts, target int64) {
	status.Goal.ProductTarget = target
	if target > 0 {
		status.Goal.ProgressPct = float64(status.Goal.QualifiedProducts) * 100 / float64(target)
	}
	if status.Traffic.AverageBytesPerProduct > 0 && target > 0 {
		status.Traffic.ProjectedTargetGB = status.Traffic.AverageBytesPerProduct * float64(target) / decimalGB
		remaining := maxInt64(target-status.Goal.QualifiedProducts, 0)
		status.Traffic.ProjectedRemainingGB = status.Traffic.AverageBytesPerProduct * float64(remaining) / decimalGB
	}
	if status.Ingest.ProductsPerMinute > 0 {
		remaining := maxInt64(target-status.Goal.QualifiedProducts, 0)
		status.Ingest.ETASeconds = float64(remaining) / float64(status.Ingest.ProductsPerMinute) * 60
	}
}

func Render(status store.Counts, opts Options, now time.Time) string {
	Enrich(&status, opts.ProductTarget)
	EnrichProxyBudget(&status, opts.ProxyBudgetGB, opts.ProxyStopReserveGB)
	var out strings.Builder
	remaining := maxInt64(status.Goal.ProductTarget-status.Goal.QualifiedProducts, 0)
	fmt.Fprintf(&out, "TRENDYOL 百万商品实时监控台  %s\n", now.Format("2006-01-02 15:04:05"))
	fmt.Fprintf(&out, "%s\n\n", strings.Repeat("=", 78))

	fmt.Fprintln(&out, "目标进度")
	fmt.Fprintf(&out, "  [%s] %6.2f%%\n", progressBar(status.Goal.ProgressPct, 42), status.Goal.ProgressPct)
	fmt.Fprintf(&out, "  合格商品 %s / %s    剩余 %s    SKU %s\n",
		formatInt(status.Goal.QualifiedProducts), formatInt(status.Goal.ProductTarget), formatInt(remaining), formatInt(status.Goal.VariantRows))
	fmt.Fprintf(&out, "  最近60秒入库 商品 %s/分钟    SKU %s/分钟    预计完成 %s\n\n",
		formatInt(status.Ingest.ProductsPerMinute), formatInt(status.Ingest.VariantsPerMinute), formatETA(status.Ingest.ETASeconds))

	if status.Backfill.TotalProducts > 0 {
		phase := "新商品抓取"
		if status.Backfill.ActiveJobs > 0 {
			phase = "旧数据补齐（新商品已暂停）"
		}
		fmt.Fprintln(&out, "旧数据详情补齐")
		fmt.Fprintf(&out, "  已补齐 %s / %s    补齐成功率 %s    活跃任务 %s    终态未补齐 %s    当前阶段 %s\n\n",
			formatInt(status.Backfill.FilledProducts), formatInt(status.Backfill.TotalProducts),
			backfillSuccessRate(status.Backfill), formatInt(status.Backfill.ActiveJobs), formatInt(status.Backfill.TerminalMissing), phase)
	}

	fmt.Fprintln(&out, "代理流量（滚动60秒，十进制 GB；统计 HTTP 响应正文）")
	fmt.Fprintf(&out, "  当前一分钟 %.6f GB    累计 %.3f GB    请求 %s/分钟\n",
		status.Traffic.LastMinuteGB, status.Traffic.TotalResponseGB, formatInt(status.Ingest.AttemptsPerMinute))
	if status.Traffic.BudgetGB > 0 {
		state := "运行中"
		if status.Traffic.StopTriggered {
			state = "已到停机线"
		}
		fmt.Fprintf(&out, "  套餐额度 %.3f GB    剩余 %.3f GB    停机线 %.3f GB（预留 %.3f GB）  %s\n",
			status.Traffic.BudgetGB, status.Traffic.RemainingGB, status.Traffic.StopThresholdGB, status.Traffic.StopReserveGB, state)
	} else {
		fmt.Fprintln(&out, "  套餐额度 未配置（PROXY_TRAFFIC_BUDGET_GB=0，自动停机未启用）")
	}
	if status.Traffic.ProjectionProducts > 0 && status.Traffic.AverageBytesPerProduct > 0 {
		fmt.Fprintf(&out, "  实测 %.3f MB/合格商品（样本 %s）\n",
			status.Traffic.AverageBytesPerProduct/1_000_000, formatInt(status.Traffic.ProjectionProducts))
		fmt.Fprintf(&out, "  预估100万商品 %.2f GB    按当前进度预计剩余 %.2f GB\n\n",
			status.Traffic.ProjectedTargetGB, status.Traffic.ProjectedRemainingGB)
	} else {
		fmt.Fprint(&out, "  预估100万商品 --（等待产生合格商品与流量样本）\n\n")
	}

	direct := status.Queues["direct"]
	fmt.Fprintln(&out, "候选发现 / 枚举补水")
	fmt.Fprintf(&out, "  最近60秒新增候选 %s    Sitemap %s    分类/搜索页 %s\n",
		formatInt(status.Ingest.CandidatesPerMinute), formatInt(status.Ingest.SitemapCandidatesPerMinute), formatInt(status.Ingest.PublicCandidatesPerMinute))
	fmt.Fprintf(&out, "  待抓队列 %s    低水位 %s    补水目标 %s\n",
		formatInt(direct["pending"]), formatInt(int64(opts.PendingLow)), formatInt(int64(opts.PendingTarget)))
	fmt.Fprintf(&out, "  高优先候选 %s    新鲜低水位 %s    新鲜目标 %s\n",
		defaultValue(status.Discovery["priority_high"], "0"), formatInt(int64(opts.FreshPendingLow)), formatInt(int64(opts.FreshPendingTarget)))
	fmt.Fprintf(&out, "  Sitemap 文件 %s    offset %s    候选任务总数 %s\n\n",
		defaultValue(status.Discovery["discovery_sitemap_index"], "1"), defaultValue(status.Discovery["discovery_sitemap_offset"], "0"), formatInt(totalJobs(status.Queues)))

	fmt.Fprintln(&out, "抓取健康")
	fmt.Fprintf(&out, "  direct  pending=%s processing=%s retry=%s done=%s skip=%s\n",
		formatInt(direct["pending"]), formatInt(direct["processing"]), formatInt(direct["retry"]), formatInt(direct["done"]), formatInt(direct["skip"]))
	fmt.Fprintf(&out, "  最近60秒 failures=%s    HTTP 403=%s 429=%s 503=%s 404=%s\n",
		formatInt(status.Ingest.FailuresPerMinute), formatInt(status.HTTP["403"]), formatInt(status.HTTP["429"]), formatInt(status.HTTP["503"]), formatInt(status.HTTP["404"]))
	fmt.Fprintf(&out, "  抓取成功率 最近60秒 %.1f%%（%s/%s）    累计 %.1f%%（%s/%s）\n",
		status.Ingest.SuccessRatePct, formatInt(status.Ingest.SuccessesPerMinute), formatInt(recentScrapeAttempts(status)),
		status.Ingest.CumulativeSuccessRatePct, formatInt(status.Ingest.CumulativeSuccesses), formatInt(status.Ingest.CumulativeAttempts))
	attempts := status.Proxy.Successes + status.Proxy.Failures
	successRate := float64(0)
	if attempts > 0 {
		successRate = float64(status.Proxy.Successes) * 100 / float64(attempts)
	}
	proxyTotal := maxInt64(opts.ConfiguredProxies, status.Proxy.Total)
	fmt.Fprintf(&out, "  代理会话=%s cooling=%s success=%.1f%%    CloudBypass=%s\n",
		formatInt(proxyTotal), formatInt(status.Proxy.Cooling), successRate, enabledText(opts.CloudBypassEnabled))
	fmt.Fprintln(&out, "\nCtrl+C 退出监控；监控命令只读，不启动抓取。")
	return out.String()
}

func backfillSuccessRate(summary store.BackfillSummary) string {
	terminal := summary.FilledProducts + summary.TerminalMissing
	if terminal == 0 {
		return "--"
	}
	return fmt.Sprintf("%.1f%%", float64(summary.FilledProducts)*100/float64(terminal))
}

func recentScrapeAttempts(status store.Counts) int64 {
	return int64(math.Round(
		status.Minute["done"] + status.Minute["retry"] + status.Minute["skip"] +
			status.Minute["parser_failure"] + status.Minute["quality_retry"] + status.Minute["challenge"],
	))
}

// EnrichProxyBudget exposes the exact same threshold used by workers. Decimal
// GB matches proxy-provider billing conventions and the existing dashboard.
func EnrichProxyBudget(status *store.Counts, budgetGB, reserveGB float64) {
	status.Traffic.BudgetGB = budgetGB
	status.Traffic.StopReserveGB = reserveGB
	if budgetGB <= 0 {
		return
	}
	status.Traffic.StopThresholdGB = math.Max(budgetGB-reserveGB, 0)
	status.Traffic.RemainingGB = math.Max(budgetGB-status.Traffic.TotalResponseGB, 0)
	status.Traffic.StopTriggered = status.Traffic.TotalResponseGB >= status.Traffic.StopThresholdGB
}

func progressBar(percent float64, width int) string {
	percent = math.Max(0, math.Min(percent, 100))
	filled := int(math.Round(percent * float64(width) / 100))
	return strings.Repeat("#", filled) + strings.Repeat("-", width-filled)
}

func formatETA(seconds float64) string {
	if seconds <= 0 || math.IsInf(seconds, 0) || math.IsNaN(seconds) {
		return "--"
	}
	d := time.Duration(seconds * float64(time.Second))
	days := d / (24 * time.Hour)
	d -= days * 24 * time.Hour
	hours := d / time.Hour
	if days > 0 {
		return fmt.Sprintf("%d天%d小时", days, hours)
	}
	minutes := d / time.Minute
	return fmt.Sprintf("%d小时%d分钟", hours, minutes)
}

func formatInt(value int64) string {
	negative := value < 0
	if negative {
		value = -value
	}
	raw := strconv.FormatInt(value, 10)
	for i := len(raw) - 3; i > 0; i -= 3 {
		raw = raw[:i] + "," + raw[i:]
	}
	if negative {
		return "-" + raw
	}
	return raw
}

func defaultValue(value, fallback string) string {
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	return value
}

func enabledText(value bool) string {
	if value {
		return "开启"
	}
	return "关闭"
}

func totalJobs(queues map[string]map[string]int64) int64 {
	var total int64
	for _, states := range queues {
		for _, count := range states {
			total += count
		}
	}
	return total
}

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
