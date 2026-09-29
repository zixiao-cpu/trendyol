# Trendyol Go 部署说明

## 本地离线验收

```bash
cp .env.example .env
bash deploy_run.sh test
bash deploy_run.sh build
bin/trendyol init
bin/trendyol status
bin/trendyol monitor --once
```

`test/init/status/monitor/export/validate/import-urls/recover/reprioritize/prepare-usd` 不访问 Trendyol；`run/discover/get` 会访问 Trendyol。

代理套餐必须在 `.env` 配置十进制 GB 总额度与安全预留：

```bash
PROXY_TRAFFIC_BUDGET_GB=100
PROXY_STOP_RESERVE_GB=2
```

`run` 达到 98 GB 本地持久化响应正文流量后会优雅退出。供应商可能另外计入协议开销或采用不同压缩口径，安全预留应按实际账单校准。

## Sitemap 预热

预热只写入 SQLite jobs/candidate_sources，不会抓 Trendyol 详情：

```bash
bin/trendyol discover
bin/trendyol reprioritize
```

Sitemap 是主候选来源；分类/搜索页可通过 `import-urls` 作为次级来源导入。Common Crawl/Kaggle 不接入。

## 小规模人工实测

自动测试完成后，由操作者决定是否执行：

```bash
PRODUCT_TARGET=20 CATEGORY_PRODUCT_CAP=2 CONCURRENCY=4 BYPASS_CONCURRENCY=0 \
PENDING_LOW_WATERMARK=5 PENDING_QUEUE_TARGET=10 SITEMAP_MAX=1 \
bash deploy_run.sh run
```

CloudBypass 默认关闭。只有明确挑战页才会进入 bypass 队列；如需开启：

```bash
CLOUDBYPASS_ENABLED=true
CLOUDBYPASS_APIKEY=replace_me
CLOUDBYPASS_MAX_ATTEMPTS=2
CLOUDBYPASS_MAX_CONCURRENCY=2
CLOUDBYPASS_BUDGET=100
```

## 恢复和交付

```bash
bash deploy_run.sh status
bash deploy_run.sh recover
bash deploy_run.sh export
bash deploy_run.sh validate
```

权威文件是 `state/trendyol.db` 及其 WAL/SHM。CSV 可随时从数据库重建；成功导出还会生成包含字节数与 SHA-256 的 manifest。

## systemd

```bash
sudo bash deploy/install.sh /opt/trendyol
sudoedit /opt/trendyol/.env
sudo systemctl enable --now trendyol
journalctl -u trendyol -f
```

安装脚本会创建缺失的 `trendyol` 系统用户/组并设置目录权限。服务仅能写入所选安装目录下的 `state` 和 `out`；传入自定义绝对安装路径时，systemd unit 会同步替换路径。

Go migrations 使用与 Python 相同的表名和字段，并只执行增量建表/加列，不删除已有 jobs、商品或断点。Python 源码仍保留用于迁移期对比。
