# 钱包连接与只读验收

Trading Execution 已经具备真实 Polymarket 钱包的本地签名和 CLOB L2 认证能力。为了避免在
PostgreSQL 账本、预占和 UNKNOWN 对账尚未整体装配时误下单，当前把钱包验收放在独立的
`cmd/walletcheck` 中：它检查 CLOB protocol V2，并通过认证后的 `/data/orders` 读取 Open Orders，
不会调用下单或撤单接口。

## 和旧实盘一致的连接方式

旧 `poly_parity/parity/execute.py` 使用：

1. EOA private key 构造 signer；
2. `address/funder_address` 指向持有资金与 shares 的钱包；
3. API key、base64 secret、passphrase 完成 L2 HMAC；
4. 三项 L2 凭证都不存在时，显式执行 create-or-derive；
5. 每个 strategy 使用各自钱包，互不 fallback。

Go 实现沿用这个边界，并使用 `execution_account_id` 选择钱包。因此新 model 或 strategy 只需要
新增 execution account 配置和上游路由，不需要修改 signer 或 CLOB adapter。

## 推荐 secrets 文件格式

文件必须放在部署平台的 secrets mount 中，不得提交到 Git：

```json
{
  "accounts": [
    {
      "execution_account_id": "model-a/strategy-v1",
      "funder_address": "0x...",
      "private_key": "0x...",
      "signature_type": 0,
      "api_key": "...",
      "api_secret": "...",
      "api_passphrase": "..."
    }
  ]
}
```

也兼容旧实盘的 keyed `wallets.json`：

```json
{
  "10": {
    "address": "0x...",
    "private_key": "0x...",
    "api_key": "...",
    "api_secret": "...",
    "api_passphrase": "..."
  }
}
```

旧格式的 key（例子里的 `10`）就是 `execution_account_id`。如果 `funder/address` 与 private key
推导出的 signer 不同，必须显式填写 `signature_type`，防止把 proxy/safe 误签成 EOA。
当前四钱包部署约定是 `#0=main`、`#1=wallet-1`、`#2=wallet-2`、
`#3=wallet-3`。这只是部署命名约定；loader 不会把 `0` 或 `wallet-0` 自动翻译成
`main`，因此钱包文件、`DECISION_CYCLE_BINDINGS_JSON` 和数据库必须使用完全一致的
literal `execution_account_id`。

## 钱包类型

- `0 EOA`：signer 和 funder 必须相同；旧实盘当前代码实际采用的默认方式。
- `1 POLY_PROXY`：legacy proxy，signer 与 funder 可以不同。
- `2 GNOSIS_SAFE`：legacy Safe，signer 与 funder可以不同。
- `3 DEPOSIT_WALLET/POLY_1271`：Go adapter 当前 fail closed，尚未完成 ERC-7739/1271 的实盘一致性验收。

不能根据地址不同自动猜 `1` 或 `2`。猜错仍可能生成结构合法但交易所拒绝的订单，因此配置缺失时
启动直接失败。

## 只读连接检查

```bash
export POLYMARKET_ACCOUNTS_FILE=/run/secrets/trading_execution/wallets.json
export POLYMARKET_CLOB_URL=https://clob.polymarket.com
export POLYMARKET_REQUEST_TIMEOUT=5s
export POLYMARKET_WALLET_CHECK_TIMEOUT=30s
go run ./cmd/walletcheck
```

成功日志只包含 execution account、signer/funder 公共地址、signature type 和 Open Orders 数量；
不会打印 private key、API secret、passphrase、HMAC 或订单签名。

默认要求文件已经包含完整 L2 凭证。如果旧钱包缺少三项凭证，可以由运维人员一次性显式启用：

```bash
export POLYMARKET_BOOTSTRAP_MISSING_API_CREDS=true
go run ./cmd/walletcheck
```

该开关会先 `POST /auth/api-key`，失败后通过 `GET /auth/derive-api-key` 读取 nonce 对应的凭证，
与旧 Python `create_or_derive_api_key()` 一致。创建 API key 会改变 Polymarket 远端凭证状态，因此
默认关闭；生产更推荐提前生成并由 secrets 系统注入。

## 从钱包连通到启用实盘

钱包连通只证明 signer 和 CLOB credentials 可用。`cmd/server` 已具备 live composition，但只有
同时满足以下条件才会启动，并且数据库全局 Kill Switch 默认保持开启：

1. PostgreSQL migrations 已在目标库执行并核对历史余额、成本和 lots；
2. 数据库账户地址与 secret 的 funder 精确一致，pUSD 初始余额、历史仓位成本和 lots 已对齐；
3. Market Universe、最新盘口、原子动态风控、strategy binding 和 Kill Switch 已配置；
4. submit timeout、Cancel Race、`/data/trades` 延迟和 Polygon `OrderFilled` 最终性由 reconciliation
   与链上证据读取器接管；
5. 账户 `closed_only=false`，pUSD 和两个 V2 Exchange allowance 正确；
6. 旧机器人已停止，并使用专用小额钱包完成明确批准的 BUY/SELL/Cancel canary。

在这些条件完成以前不能解除全局 Kill Switch。进程即使以 live 配置启动，新 Place 也会 fail closed；
Cancel/Get 和只读 reconciliation 则始终保留，便于降低风险。
