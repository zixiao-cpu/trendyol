# Trendyol Go 生产采集器

这个目录同时保留 Python 参考实现与新的 Go 生产实现。Go 版本不是逐行翻译：它复用相同的 SQLite/CSV 数据契约，重新实现了浏览器传输、代理生命周期、任务调度、渐进发现、CloudBypass 分流、监控与导出。

## 数据与业务规则

- SQLite WAL 是唯一权威状态源，兼容现有 `state/trendyol.db`；CSV 是可重建交付物。
- jobs 状态保持 `pending / processing / retry / done / skip`，带租约、指数退避和 `job_events`。
- 数据表保持 `products / variants / sellers / offers / reviews`；保存业务数据和标记 job done 在同一事务完成。
- `candidate_sources` 记录候选 ID 来自 Sitemap、分类/搜索页或手工文件；同一个 `product_id` 仍由 jobs 全局去重。
- 固定 30 列、每行一个 Trendyol SKU；`item_number` 唯一，`product_group_id` 表示同款分组。
- 采集目标按唯一 `product_id` 计数，默认 `PRODUCT_TARGET=1000000`；一个商品的全部合格 SKU 在同一事务完整保存，SKU 数量不占用商品目标。
- `CATEGORY_PRODUCT_CAP` 按商品而不是 SKU 限制分类数量，百万目标默认设为 `0`（关闭）。旧的 `TOTAL_TARGET / CATEGORY_CAP` 仍可读取，但建议迁移到新名称。
- 商品总数和分类商品数由 SQLite 事务内计数器维护，`status --json` 会返回 `product_goal`，长期运行不需要反复全表扫描。
- 代理响应正文流量在 SQLite 中持久累计；配置 `PROXY_TRAFFIC_BUDGET_GB` 与 `PROXY_STOP_RESERVE_GB` 后，达到“套餐额度−安全预留”即停止新请求、保存所有在途结果并正常退出。systemd 使用 `Restart=on-failure`，不会因额度停机而重启。
- 运行前若存在 `prepare-content-backfill` 产生的优先级 400 旧商品任务，调度器进入严格补齐阶段：只领取旧商品（包括延迟重试），暂停 Sitemap/分类页发现和所有新候选；旧任务全部进入终态后自动恢复新商品采集。
- `color / capacity / material` 永远存在，缺失时为空。
- 页面上的 SKU TRY 价格按配置快照 `TRY_PER_USD` 换算，`selling_price` 与 `currency` 统一保存为 USD；只保留舍入到美分后严格大于 `MIN_SELLING_PRICE_USD=20` 的 SKU。多 SKU 不回退到 winnerVariant 的其他 SKU 价格。
- 只在有买家可理解的主题和值时生成 variation；绝不把 `itemNumber` 当规格。缺规格先换会话重试一次，再以数据库 `variation_incomplete` 降级。
- 评论只取详情响应内嵌内容，最多 10 条；没有额外评论请求。
- `description` 保存详情页商品区域的完整可读纯文本，包括商品简介、商品说明、补充信息、商家、价格/折扣、库存提示、条码、评分、收藏、促销、配送和退货等；不保存导航、页脚、脚本或 HTML/CSS 源码。结构化属性继续保存在 `features/specs`。

## 运行架构

```text
渐进 Sitemap / 分类搜索页
              ↓ 低水位补水并持久化 sitemap index + offset
SQLite WAL jobs（全局去重、租约、retry_at）
        ↓                              ↓
Direct Dispatcher                CloudBypass Dispatcher
批量 claim + 精确等待             批量 claim + 小并发/预算
        ↓ bounded channel               ↓
独占 TLS Session Worker          独立 CloudBypass Worker
固定区域代理/Cookie Jar           仅明确挑战页进入
        └──────────────┬───────────────┘
                       ↓
            单 Result Writer / 批量短事务
                       ↓
       products / variants / sellers / offers / reviews
                       ↓
        流式 CSV 校验、fsync、原子替换、SHA-256 manifest
```

每个 Worker 独占 `tls-client` Chrome 131 TLS/H2 Client 和 Cookie Jar。`Session.Rotate(reason)` 会关闭旧连接、冷却旧出口、生成新 SID、绑定新代理并重建 Client/Jar；达到 sticky page 数量或遇到挑战、429、503、代理故障时分类轮换。当前服务器模板使用美国区域，供应商按随机 SID 分配美国城市出口。

CloudBypass 默认关闭。只有 HTML 明确包含 CAPTCHA、Challenge、Verify Human、Turnstile 等特征才会转入专用队列；普通 403/429/503 只换代理并延迟重试，HTTP 200 解析失败保存脱敏 Fixture、换会话重试一次，仍失败则 `parser_incompatible`。

## 候选来源

- Trendyol Sitemap 是全量主来源，优先级 300，并按 sitemap index/offset 渐进、可恢复地补充。
- Trendyol 分类页、搜索页是次级来源，优先级 200；通过 `import-urls` 导入页面后，在 Sitemap 用尽时补充。
- Common Crawl 和 Kaggle 不接入候选链路，也不保留对应下载/导入命令。

最终图片 URL、详情、分类链路、规格/SKU、USD 价格和最多 10 条内嵌评论，必须重新访问 Trendyol 详情页解析并按 `selling_price > 20 USD` 验证。

## 构建和离线测试

需要 Go 1.25 或更高版本：

```bash
go build ./cmd/trendyol
go test ./...
go vet ./...
```

测试只读取 `testdata/fixtures`，不会访问 Trendyol、代理或 CloudBypass。Fixture 覆盖真实脱敏单 SKU、多规格/颜色、多个报价、SKU 不同价格、缺价格、缺规格、无评论、下架、404、200 挑战及状态结构变化。`testdata/baseline/historical_single.csv` 是 Python 30 列输出基线。

## 配置

```bash
cp .env.example .env
chmod 600 .env
# 填写代理；CloudBypass 如不使用保持 false
bash deploy_run.sh build
```

启动时会打印脱敏有效配置，只显示代理数量和是否配置模板，不输出代理密码或 API Key。完整配置见 [.env.example](.env.example)。

## CLI

```bash
bin/trendyol init
bin/trendyol status
bin/trendyol monitor --interval 2s
bin/trendyol import-urls seeds.txt
bin/trendyol recover
bin/trendyol recover-worker <worker-id>
bin/trendyol reprioritize
bin/trendyol prepare-usd
bin/trendyol prepare-content-backfill
bin/trendyol export
bin/trendyol validate
```

`monitor` 是只读实时监控台，默认每 2 秒刷新，目标取 `PRODUCT_TARGET`（默认
1,000,000）。它显示滚动 60 秒代理响应正文 GB、按全部实测流量/合格商品推算的
百万商品 GB、每分钟商品/SKU 入库、Sitemap 与分类/搜索页候选补水、队列水位、
错误和数据质量，并显示代理套餐总额度、已用、剩余、安全预留和自动停机线。`monitor --once`
只打印一帧，适合巡检脚本。流量数字统计 HTTP 响应正文，不含请求头、TLS 和代理商
计费开销，因此应设置足够的 `PROXY_STOP_RESERVE_GB`，不要把停机线直接压到供应商余额 0。

升级到完整详情纯文本后，先在服务停止状态执行一次
`prepare-content-backfill`。它只把 `description` 为空的既有商品高优先级重新排队，
保留当前商品/SKU 数据，后续成功刷新后再原子更新详情内容。

以下命令会实际访问网络，测试过程不会自动执行：

```bash
bin/trendyol discover
bin/trendyol run
bin/trendyol get <url-or-product-id>
```

`run` 同时运行低水位发现和抓取，不会在启动时一次导入全部 301 个 Sitemap。`CLOUDBYPASS_ENABLED=false` 时 bypass Worker 为 0，并把遗留 bypass 任务安全退回 direct retry。

`TRY_PER_USD=48.066` 是 2026-08-21 的 Frankfurter/ECB 汇率快照。长任务使用固定快照保证同批数据可复现；更新汇率时应同步修改 `FX_RATE_AS_OF`，然后执行一次 `prepare-usd` 重新排入旧价格过滤任务。

导出生成：

- `out/TRENDYOL_SKU_DATA.csv`
- `out/TRENDYOL_SKU_DATA.csv.manifest.json`，包含行数、文件字节数、SHA-256 和评论查询次数。

## 部署

```bash
sudo bash deploy/install.sh /opt/trendyol
sudo systemctl enable --now trendyol
sudo systemctl status trendyol
```

Python 文件和既有 SQLite 数据不会被安装或迁移流程删除。完成实际小样本 Python/Go 对比前，不应删除 Python 版本。
