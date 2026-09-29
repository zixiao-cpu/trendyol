param(
    [ValidateRange(5, 3600)]
    [int]$RefreshSeconds = 15
)

$ErrorActionPreference = 'Stop'
$scriptRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$stateDir = Join-Path $scriptRoot 'state'
$htmlPath = Join-Path $stateDir 'trendyol-monitor.html'
$sshKey = 'C:\Users\mengxiaoxiao\Downloads\trendyol.pem'
$sshTarget = 'ubuntu@101.32.207.141'
$mutex = [Threading.Mutex]::new($false, 'Local\TrendyolHtmlMonitor_101_32_207_141')
$ownsMutex = $false

function ConvertTo-HtmlText([object]$Value) {
    return [Net.WebUtility]::HtmlEncode([string]$Value)
}

function Format-Count([object]$Value) {
    if ($null -eq $Value) { return '--' }
    return '{0:N0}' -f [double]$Value
}

function Format-Decimal([object]$Value, [int]$Places = 3) {
    if ($null -eq $Value) { return '--' }
    return ([double]$Value).ToString("N$Places")
}

function Write-DashboardFile([string]$Content) {
    if (-not (Test-Path -LiteralPath $stateDir)) {
        [IO.Directory]::CreateDirectory($stateDir) | Out-Null
    }
    [IO.File]::WriteAllText($htmlPath, $Content, [Text.UTF8Encoding]::new($false))
}

$probePython = @'
import json, sqlite3, time

DB = "file:/opt/trendyol/state/trendyol.db?mode=ro"
db = sqlite3.connect(DB, uri=True, timeout=10)
db.execute("PRAGMA query_only=ON")
q = db.cursor()

queues = {name: {state: 0 for state in ("pending", "processing", "retry", "done", "skip")}
          for name in ("direct", "cloudbypass")}
for queue, state, count in q.execute("SELECT queue_name,state,COUNT(*) FROM jobs GROUP BY queue_name,state"):
    queues.setdefault(queue, {})[state] = count

def metadata_int(key, fallback=0):
    row = q.execute("SELECT value FROM metadata WHERE key=?", (key,)).fetchone()
    try:
        return int(row[0]) if row else fallback
    except (TypeError, ValueError):
        return fallback

products = metadata_int("accepted_product_count")
variants = metadata_int("accepted_variant_count")
minute_ago = time.time() - 60
minute = {}
response_bytes_minute = 0
attempts_minute = 0
for kind, count, value, body_bytes in q.execute(
    "SELECT kind,COUNT(*),COALESCE(SUM(value),0),COALESCE(SUM(bytes),0) "
    "FROM metric_events WHERE created_at>=? GROUP BY kind", (minute_ago,)):
    minute[kind] = {"count": count, "value": value}
    response_bytes_minute += body_bytes
    if not kind.startswith("reason:"):
        attempts_minute += count

outcome_kinds = ("done", "retry", "skip", "parser_failure", "quality_retry", "challenge")
scrape_attempts_minute = sum(int(minute.get(kind, {}).get("count", 0)) for kind in outcome_kinds)
scrape_successes_minute = int(minute.get("done", {}).get("count", 0))

def metric_total(key):
    row = q.execute("SELECT value FROM metric_totals WHERE key=?", (key,)).fetchone()
    return float(row[0]) if row else 0.0

total_bytes = metric_total("response_bytes")
http = {code: int(metric_total("http:" + code)) for code in ("403", "404", "429", "503")}

backfill = q.execute("""
SELECT COUNT(*),
       COALESCE(SUM(CASE WHEN TRIM(p.description)<>'' THEN 1 ELSE 0 END),0),
       COALESCE(SUM(CASE WHEN j.state IN ('pending','processing','retry') THEN 1 ELSE 0 END),0),
       COALESCE(SUM(CASE WHEN j.state IN ('done','skip') AND TRIM(p.description)='' THEN 1 ELSE 0 END),0)
FROM jobs j JOIN products p ON p.product_id=j.product_id
WHERE j.priority>=400
""").fetchone()

proxy = q.execute("""
SELECT COUNT(*),
       COALESCE(SUM(CASE WHEN cooldown_until>? THEN 1 ELSE 0 END),0),
       COALESCE(SUM(successes),0),COALESCE(SUM(failures),0)
FROM proxy_stats
""", (time.time(),)).fetchone()

discovery = {key: value for key, value in q.execute(
    "SELECT key,value FROM metadata WHERE key LIKE 'discovery_%'")}
priority = q.execute("""
SELECT COALESCE(SUM(CASE WHEN priority>0 THEN 1 ELSE 0 END),0),
       COALESCE(SUM(CASE WHEN priority=0 THEN 1 ELSE 0 END),0),
       COALESCE(SUM(CASE WHEN priority<0 THEN 1 ELSE 0 END),0)
FROM jobs WHERE state IN ('pending','retry')
""").fetchone()

env = {}
try:
    with open("/opt/trendyol/.env", "r", encoding="utf-8") as handle:
        for raw in handle:
            raw = raw.strip()
            if raw and not raw.startswith("#") and "=" in raw:
                key, value = raw.split("=", 1)
                env[key.strip()] = value.strip().strip("'\"")
except OSError:
    pass

def env_float(key, fallback=0.0):
    try:
        return float(env.get(key, fallback))
    except (TypeError, ValueError):
        return fallback

def env_int(key, fallback=0):
    try:
        return int(env.get(key, fallback))
    except (TypeError, ValueError):
        return fallback

budget_gb = env_float("PROXY_TRAFFIC_BUDGET_GB")
reserve_gb = env_float("PROXY_STOP_RESERVE_GB")
used_gb = total_bytes / 1_000_000_000
threshold_gb = max(budget_gb - reserve_gb, 0.0) if budget_gb > 0 else 0.0
remaining_gb = max(budget_gb - used_gb, 0.0) if budget_gb > 0 else 0.0
target = env_int("PRODUCT_TARGET", 1_000_000)
proxy_attempts = proxy[2] + proxy[3]
cumulative_successes = int(metric_total("outcome:done"))
cumulative_attempts = sum(int(metric_total("outcome:" + kind)) for kind in outcome_kinds)
backfill_terminal = backfill[1] + backfill[3]

result = {
    "products": products,
    "variants": variants,
    "target": target,
    "progress_pct": (products * 100.0 / target) if target > 0 else 0.0,
    "queues": queues,
    "minute": {
        "products": int(minute.get("done", {}).get("count", 0)),
        "variants": int(minute.get("done", {}).get("value", 0)),
        "attempts": int(attempts_minute),
        "response_gb": response_bytes_minute / 1_000_000_000,
    },
    "success": {
        "minute_successes": scrape_successes_minute,
        "minute_attempts": scrape_attempts_minute,
        "minute_pct": (scrape_successes_minute * 100.0 / scrape_attempts_minute) if scrape_attempts_minute else 0.0,
        "cumulative_successes": cumulative_successes,
        "cumulative_attempts": cumulative_attempts,
        "cumulative_pct": (cumulative_successes * 100.0 / cumulative_attempts) if cumulative_attempts else 0.0,
        "backfill_pct": (backfill[1] * 100.0 / backfill_terminal) if backfill_terminal else 0.0,
    },
    "traffic": {
        "used_gb": used_gb,
        "budget_gb": budget_gb,
        "remaining_gb": remaining_gb,
        "reserve_gb": reserve_gb,
        "threshold_gb": threshold_gb,
        "stop_triggered": budget_gb > 0 and used_gb >= threshold_gb,
    },
    "backfill": {
        "total": backfill[0], "filled": backfill[1],
        "active": backfill[2], "terminal_missing": backfill[3],
    },
    "proxy": {
        "sessions": proxy[0], "cooling": proxy[1],
        "success_pct": (proxy[2] * 100.0 / proxy_attempts) if proxy_attempts else 0.0,
    },
    "http": http,
    "discovery": {
        "sitemap_index": discovery.get("discovery_sitemap_index", "--"),
        "sitemap_offset": discovery.get("discovery_sitemap_offset", "--"),
        "priority_high": priority[0], "priority_neutral": priority[1], "priority_low": priority[2],
    },
}
print(json.dumps(result, ensure_ascii=False, separators=(",", ":")))
db.close()
'@

$probeBase64 = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($probePython))

function Get-RemoteSnapshot {
    if (-not (Test-Path -LiteralPath $sshKey)) {
        throw "SSH 密钥不存在：$sshKey"
    }
    $remoteCommand = "printf 'SERVICE='; systemctl is-active trendyol || true; sudo -u trendyol python3 -c `"import base64;exec(base64.b64decode('$probeBase64'))`""
    $output = @(& ssh.exe -i $sshKey -o BatchMode=yes -o ConnectTimeout=10 $sshTarget $remoteCommand 2>&1 | ForEach-Object { [string]$_ })
    if ($LASTEXITCODE -ne 0) {
        throw "SSH 读取失败（exit=$LASTEXITCODE）：$($output -join ' ')"
    }
    $serviceLine = $output | Where-Object { $_ -like 'SERVICE=*' } | Select-Object -Last 1
    $jsonLine = $output | Where-Object { $_.TrimStart().StartsWith('{') } | Select-Object -Last 1
    if (-not $jsonLine) {
        throw "服务器没有返回监控 JSON：$($output -join ' ')"
    }
    return [pscustomobject]@{
        Service = if ($serviceLine) { $serviceLine.Substring(8).Trim() } else { 'unknown' }
        Data = $jsonLine | ConvertFrom-Json
    }
}

function New-DashboardHtml($Snapshot) {
    $data = $Snapshot.Data
    $service = [string]$Snapshot.Service
    $serviceText = if ($service -eq 'active') { '抓取运行中' } elseif ($service -eq 'inactive') { '抓取已停止' } else { $service }
    $serviceClass = if ($service -eq 'active') { 'running' } elseif ($service -eq 'inactive') { 'stopped' } else { 'warning' }
    $direct = $data.queues.direct
    $backfill = $data.backfill
    $traffic = $data.traffic
	$success = $data.success
    $backfillPhase = if ([int64]$backfill.active -gt 0) { '旧数据补齐中，新商品暂停' } else { '旧数据阶段已结束' }
    $budgetEnabled = [double]$traffic.budget_gb -gt 0
    $budgetText = if ($budgetEnabled) { "$(Format-Decimal $traffic.remaining_gb) GB" } else { '不限额度' }
    $updated = Get-Date -Format 'yyyy-MM-dd HH:mm:ss'
    $progressWidth = [Math]::Max(0, [Math]::Min(100, [double]$data.progress_pct))
    $proxyState = if (-not $budgetEnabled) { '自动停机已取消' } elseif ($traffic.stop_triggered) { '已到停机线' } else { '额度正常' }
    $proxyDetail = if ($budgetEnabled) { "阈值 $(Format-Decimal $traffic.threshold_gb) GB · 预留 $(Format-Decimal $traffic.reserve_gb) GB" } else { "达到 $(Format-Count $data.target) 个商品目标后停止" }

    return @"
<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta http-equiv="refresh" content="$RefreshSeconds">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>Trendyol 实时监控台</title>
  <style>
    :root { color-scheme: dark; --bg:#07111f; --panel:#0e1b2e; --line:#203451; --text:#ecf4ff; --muted:#8ea3bf; --cyan:#38bdf8; --green:#34d399; --amber:#fbbf24; --red:#fb7185; }
    * { box-sizing:border-box; }
    body { margin:0; background:radial-gradient(circle at top left,#102b48 0,#07111f 42%); color:var(--text); font:14px/1.5 "Segoe UI","Microsoft YaHei",sans-serif; min-height:100vh; }
    main { max-width:1280px; margin:auto; padding:28px; }
    header { display:flex; justify-content:space-between; gap:16px; align-items:flex-start; margin-bottom:22px; }
    h1 { margin:0 0 5px; font-size:28px; letter-spacing:.3px; }
    .sub,.muted { color:var(--muted); }
    .pill { display:inline-flex; align-items:center; border:1px solid var(--line); border-radius:999px; padding:7px 12px; font-weight:700; }
    .pill.running { color:var(--green); background:#0a2a26; } .pill.stopped { color:var(--amber); background:#2b2411; } .pill.warning { color:var(--red); }
    .grid { display:grid; grid-template-columns:repeat(4,minmax(0,1fr)); gap:14px; }
    .card { background:linear-gradient(145deg,rgba(16,32,53,.97),rgba(10,23,40,.97)); border:1px solid var(--line); border-radius:16px; padding:18px; box-shadow:0 16px 40px rgba(0,0,0,.18); }
    .span2 { grid-column:span 2; } .span4 { grid-column:span 4; }
    .label { color:var(--muted); font-size:13px; margin-bottom:6px; }
    .value { font-size:27px; font-weight:760; letter-spacing:.2px; }
    .value.small { font-size:18px; }
    .accent { color:var(--cyan); } .good { color:var(--green); } .warn { color:var(--amber); } .bad { color:var(--red); }
    .bar { height:10px; background:#071321; border:1px solid #1d3451; border-radius:99px; overflow:hidden; margin:12px 0 8px; }
    .bar > i { display:block; width:$($progressWidth.ToString('0.00'))%; height:100%; background:linear-gradient(90deg,#0284c7,#38bdf8,#34d399); }
    table { width:100%; border-collapse:collapse; }
    td { padding:8px 0; border-bottom:1px solid rgba(32,52,81,.55); } td:last-child { text-align:right; font-weight:700; }
    tr:last-child td { border-bottom:0; }
    footer { color:var(--muted); margin-top:18px; text-align:center; }
    @media(max-width:900px){ .grid{grid-template-columns:repeat(2,minmax(0,1fr))}.span4{grid-column:span 2} }
    @media(max-width:560px){ main{padding:16px}.grid{grid-template-columns:1fr}.span2,.span4{grid-column:span 1}header{display:block}.pill{margin-top:10px} }
  </style>
</head>
<body><main>
  <header>
    <div><h1>Trendyol 实时监控台</h1><div class="sub">服务器 101.32.207.141 · 每 $RefreshSeconds 秒自动刷新 · 更新时间 $updated</div></div>
    <span class="pill $serviceClass">$serviceText</span>
  </header>
  <section class="grid">
    <article class="card span2">
      <div class="label">百万商品目标</div><div class="value accent">$(Format-Count $data.products) / $(Format-Count $data.target)</div>
      <div class="bar"><i></i></div><div class="muted">进度 $(Format-Decimal $data.progress_pct 2)% · SKU $(Format-Count $data.variants)</div>
    </article>
    <article class="card"><div class="label">最近 60 秒商品</div><div class="value good">$(Format-Count $data.minute.products)</div><div class="muted">SKU $(Format-Count $data.minute.variants) / 分钟</div></article>
    <article class="card"><div class="label">最近 60 秒请求</div><div class="value">$(Format-Count $data.minute.attempts)</div><div class="muted">响应正文 $(Format-Decimal $data.minute.response_gb 6) GB</div></article>

    <article class="card span2">
      <div class="label">旧数据详情补齐</div><div class="value $(if ([int64]$backfill.active -gt 0) {'warn'} else {'good'})">$(Format-Count $backfill.filled) / $(Format-Count $backfill.total)</div>
      <div class="muted">补齐成功率 $(Format-Decimal $success.backfill_pct 1)% · 活跃任务 $(Format-Count $backfill.active) · 终态未补齐 $(Format-Count $backfill.terminal_missing)</div>
      <div class="value small" style="margin-top:10px">$backfillPhase</div>
    </article>
    <article class="card"><div class="label">代理额度</div><div class="value good">$budgetText</div><div class="muted">已统计 $(Format-Decimal $traffic.used_gb) GB</div></article>
    <article class="card"><div class="label">代理停机保护</div><div class="value small $(if ($traffic.stop_triggered) {'bad'} else {'good'})">$proxyState</div><div class="muted">$proxyDetail</div></article>

    <article class="card span2"><div class="label">Direct 队列</div><table>
      <tr><td>pending</td><td>$(Format-Count $direct.pending)</td></tr><tr><td>processing</td><td>$(Format-Count $direct.processing)</td></tr>
      <tr><td>retry</td><td>$(Format-Count $direct.retry)</td></tr><tr><td>done / skip</td><td>$(Format-Count ([int64]$direct.done + [int64]$direct.skip))</td></tr>
    </table></article>
    <article class="card"><div class="label">抓取成功率（最近 60 秒）</div><div class="value good">$(Format-Decimal $success.minute_pct 1)%</div><div class="muted">成功 $(Format-Count $success.minute_successes) / 尝试 $(Format-Count $success.minute_attempts)</div><div class="muted">累计 $(Format-Decimal $success.cumulative_pct 1)%（$(Format-Count $success.cumulative_successes) / $(Format-Count $success.cumulative_attempts)）</div><div class="muted">代理健康 $(Format-Decimal $data.proxy.success_pct 1)% · cooling $(Format-Count $data.proxy.cooling)</div></article>
    <article class="card"><div class="label">HTTP 累计异常</div><table><tr><td>403 / 429</td><td>$(Format-Count ([int64]$data.http.'403' + [int64]$data.http.'429'))</td></tr><tr><td>503 / 404</td><td>$(Format-Count ([int64]$data.http.'503' + [int64]$data.http.'404'))</td></tr></table></article>

    <article class="card span4"><div class="label">候选发现</div><div class="value small">Sitemap #$(ConvertTo-HtmlText $data.discovery.sitemap_index) · offset $(ConvertTo-HtmlText $data.discovery.sitemap_offset)</div><div class="muted">高优先 $(Format-Count $data.discovery.priority_high) · 普通 $(Format-Count $data.discovery.priority_neutral) · 低优先 $(Format-Count $data.discovery.priority_low)</div></article>
  </section>
  <footer>这是只读监控页面；关闭随 CMD 打开的控制台窗口即可停止更新。</footer>
</main></body></html>
"@
}

function New-ErrorHtml([string]$Message) {
    $safe = ConvertTo-HtmlText $Message
    $updated = Get-Date -Format 'yyyy-MM-dd HH:mm:ss'
    return @"
<!doctype html><html lang="zh-CN"><head><meta charset="utf-8"><meta http-equiv="refresh" content="$RefreshSeconds"><title>Trendyol 监控连接失败</title>
<style>body{margin:0;background:#07111f;color:#ecf4ff;font:16px/1.6 "Segoe UI","Microsoft YaHei",sans-serif;display:grid;place-items:center;min-height:100vh}.box{max-width:780px;background:#0e1b2e;border:1px solid #3b2940;border-radius:18px;padding:28px;box-shadow:0 18px 50px #0006}h1{color:#fb7185}code{color:#fbbf24;word-break:break-word}</style></head>
<body><div class="box"><h1>暂时无法读取服务器</h1><p>页面会每 $RefreshSeconds 秒自动重试。</p><code>$safe</code><p>更新时间：$updated</p></div></body></html>
"@
}

try {
    $ownsMutex = $mutex.WaitOne(0)
    if (-not $ownsMutex) {
        if (Test-Path -LiteralPath $htmlPath) { Start-Process $htmlPath }
        Write-Host '监控更新程序已经在运行，已重新打开浏览器页面。' -ForegroundColor Yellow
        exit 0
    }

    $browserOpened = $false
    Write-Host "Trendyol HTML 监控已启动：每 $RefreshSeconds 秒刷新。" -ForegroundColor Cyan
    Write-Host '关闭此窗口即可停止更新；监控只读，不控制抓取服务。' -ForegroundColor DarkGray
    while ($true) {
        try {
            $snapshot = Get-RemoteSnapshot
            Write-DashboardFile (New-DashboardHtml $snapshot)
            Write-Host "[$(Get-Date -Format 'HH:mm:ss')] 已刷新：商品 $(Format-Count $snapshot.Data.products)，旧数据活跃 $(Format-Count $snapshot.Data.backfill.active)"
        } catch {
            Write-DashboardFile (New-ErrorHtml $_.Exception.Message)
            Write-Warning $_.Exception.Message
        }
        if (-not $browserOpened) {
            Start-Process $htmlPath
            $browserOpened = $true
        }
        Start-Sleep -Seconds $RefreshSeconds
    }
} finally {
    if ($ownsMutex) { $mutex.ReleaseMutex() }
    $mutex.Dispose()
}
