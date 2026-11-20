# Polymarket CLOB V2 执行适配器

实现位置：`internal/adapter/polymarket`。该包只完成凭证选择、订单构造、签名、HTTP 调用、精度校验、限流与错误归一化，不包含 probability、Edge、止盈止损或仓位算法。

## 钱包和凭证边界

每个 `execution_account_id` 必须精确映射到一个 `TradingAccount`：

- `DigestSigner`：EOA、本地 signer、HSM 或 KMS 的统一签名接口；
- `FunderAddress`：实际持有 pUSD/shares 的钱包；
- `SignatureType`：EOA、POLY_PROXY、GNOSIS_SAFE、DEPOSIT_WALLET/POLY_1271；
- API key、base64url secret、passphrase：用于 L2 HMAC；
- 不存在默认钱包或 fallback，找不到账户直接拒绝。

`StaticCredentialProvider` 适用于由进程外 secrets 系统注入后的账户对象。代码不把私钥、API secret、签名或完整签名请求写入订单日志；attempt 只保存非敏感 SHA-256 fingerprint。

确认成交的真实现金只认 Polygon V2 Exchange `OrderFilled` 日志中的整数
`makerAmountFilled/takerAmountFilled/fee`，并把 chain、exchange、transaction、block、log index、
order hash 和确认数作为不可变证据持久化。`/data/trades.fee_rate_bps` 只是上游费率元数据，
`maker_orders[].fee_rate_bps` 也不是 builder fee，二者都不能冒充实际扣款。当前默认 zero builder
可由事件直接证明；非零 `BuilderCode` 如果没有独立、权威且能与链上 total fee 对账的拆分证据，
成交会 fail closed，而不是猜测 platform/builder 分摊。

当前支持 V2 signature type `0/1/2/3`。`POLY_1271 (3)` 使用官方 V2 的 Deposit Wallet
`TypedDataSign` 外层摘要和 ERC-7739 trailer；订单的 maker/signer 都是经过确定性地址校验的
Deposit Wallet，底层 EOA 只签外层摘要，不能把它误当普通 EIP-712 签名。

## 签名和请求

适配器只连接 CLOB protocol V2，并在首次下单前检查 `/version == 2`。订单 EIP-712 domain 为：

```text
name              = Polymarket CTF Exchange
version           = 2
chainId           = 137
verifyingContract = standard V2 exchange 或 neg-risk V2 exchange
```

签名结构包含 `salt/maker/signer/tokenId/makerAmount/takerAmount/side/signatureType/timestamp/metadata/builder`。`expiration` 只进入提交 JSON，不进入 V2 typed data。

每个 L2 请求使用 `POLY_ADDRESS/POLY_SIGNATURE/POLY_TIMESTAMP/POLY_API_KEY/POLY_PASSPHRASE`。HMAC 消息为 `timestamp + method + requestPath + exactBodyBytes`。JSON 只序列化一次，HMAC 和 HTTP 共用相同的 byte slice，避免字段顺序或空格变化导致鉴权失败。

## 精度与最小金额

价格、shares 和金额始终使用十进制字符串及 `big.Rat/big.Int`，不经过 `float32/float64`。

- price 必须是最新 tick size 的整数倍；
- shares 当前最多 2 位小数，与官方 V2 rounding table 一致；
- 策略 BUY 输入的 shares 在金额、最小下单量和风控校验前按四舍五入转为整数；
- `price × shares` 必须能按对应 tick 的 amount precision 精确表达；
- raw maker/taker amount 和 CLOB order/trade quantity 使用 6 位 token decimals；例如 wire
  `100000000` 必须显式解码为 `100` shares；
- BUY size 必须不低于 `/book.min_order_size`；SELL 不做该本地检查，由交易所决定是否接受，不能因此承诺小额一定成交；
- BUY notional 默认不得低于 `1 pUSD`，可配置但不能为零；
- FAK/FOK BUY 的 maker notional 最多 4 位小数；SELL 的 taker notional 保持最多 4 位小数；
- 除上述明确的 BUY 整数化外，不合法的策略数量直接拒绝；Go 不会为了通过交易所校验而二次改小 shares 或改变方向。

旧 `execute.py` 用 float 先算 USD、再除以价格、再交给 SDK 二次 round-down，可能把 `16.90` 变成 `16.89`。新实现直接生成 6-decimal 整数，例如 `16.90 shares → 16900000`，彻底移除双重舍入。

原生支持 `GTC/GTD/FAK/FOK`。Polymarket 没有 IOC 类型，adapter 通过 `SupportsTimeInForce(IOC)=false`
声明这一点（生产装配里的每层 venue 封装——conditional funding、geoblock eligibility、CLOB heartbeat、
placement readiness——都必须透传这个能力接口，否则执行层只看到最外层封装、误以为 IOC 是原生的，
模拟 IOC 的未成交余量就会一直挂在簿上），执行层据此模拟 IOC：订单以 `GTC` 限价签名并提交，BUY 数量为 Market Validation 记录的
`executable_size`（最新盘口在 `worst_price` 内可成交的量，不超过策略 `size`，不低于 `min_order_size`），
下单响应返回后执行层立刻撤掉未成交部分。IOC 绝不会被改成 FAK：FAK/FOK BUY 的 maker notional 是 pUSD 预算，
盘口好于限价时会买到多于 `size` 的 shares；GTC 按股数成交，最多买 `size` 股，价差体现为少花钱。

## main 钱包回撤买入

回撤监控使用账户交易盈亏。每次采样将账本里的已实现盈亏，与未平仓份额按当时报价计算的市值减剩余成本相加。平仓、市场结算和赎回依照原账本的入账状态计算。窗口回撤为同口径历史样本的盈亏高点减本期盈亏。报价不全时，本次采样中止。

升级监控前应用 `0052_wallet_drawdown_trading_pnl.sql`。历史总资产采样保存在原表，盈亏监控从首条新采样开始累计基线。

`walletdrawdown` 通过 `WALLET_DRAWDOWN_EXECUTION_URL` 启用交易。该变量使用执行服务的本机 HTTP 地址，命令复用 `EXECUTION_API_TOKEN`。启用前应用迁移 `0049_wallet_drawdown_order_operations.sql`，并在交易服务配置中为 `main` 声明 `wallet_drawdown` 下单权限。使用跟买时同时声明跟买账户的权限。

```dotenv
POLYMARKET_API_ORDER_ACCOUNTS_JSON=[{"execution_account_id":"main","strategy_id":"wallet_drawdown"},{"execution_account_id":"wallet-10","strategy_id":"wallet_drawdown"}]
```

钱包文件须包含配置中的所有 Polymarket 预测账户与 API 下单账户。若 `wallet-10` 只用于 API 下单，它只出现在钱包文件和 API 下单配置里。预测调度从 `DECISION_CYCLE_ACCOUNTS_JSON` 读账户。两份配置可给同一账户授予不同策略权限，例如 `main` 可同时处理预测订单和回撤订单。

给每项 API 下单权限启用一条 `(strategy_id, execution_account_id)` 风控授权。账户需要有效的实盘风控和账本，启动后按正常流程检查余额、订单与对账状态。以下示例适用于已应用 `0053_account_orderbook_snapshots.sql` 的数据库，且假定每个账户还没有 `wallet_drawdown` 授权。

```sql
INSERT INTO execution_strategy_bindings (model_id, strategy_id, execution_account_id, enabled)
VALUES ('wallet_drawdown', 'wallet_drawdown', 'main', TRUE),
       ('wallet_drawdown', 'wallet_drawdown', 'wallet-10', TRUE)
ON CONFLICT (model_id, strategy_id, execution_account_id) DO UPDATE SET enabled = TRUE;
```

每轮回撤首次严格超过 50 美元且满足告警阈值时，由成功写入该采样槽的进程调用 `thinmarket.Service.Find`。它按候选顺序经 `POST /api/v1/orders` 提交原生 `BUY LIMIT FOK`。最高价格是卖一价，交易预算是卖一价乘该档份额，手续费另计。成交价更好时实际买入份额可以增加。只有执行服务返回明确拒单且零成交时才换下一盘。成交或状态待确认时结束本轮提交，由现有订单协调器继续核对。任务输出的 `trade` 包含尝试顺序和订单状态。

同一轮持续超限只触发一次。同一采样槽重跑可补发飞书通知。`--dry-run` 只采样和计算，交易开关取决于上述环境变量。

每次尝试在调用下单接口前写入 `wallet_drawdown_order_operations`。表记录回撤采样槽、顺序、买卖方向、盘口和提交结果。进程在提交期间中断时，行保留在 `PREPARED`。接口未能返回可关联订单时，行记为 `SUBMIT_UNCONFIRMED`。可关联到执行订单时，以执行订单的最新状态和成交记录为准。

```sql
SELECT op.slot_at, s.drawdown, op.attempt, op.side, op.market_id, op.token_id,
       op.price, op.size, op.price * op.size AS requested_notional, op.submission_state,
       o.order_id, o.status, o.filled_size, o.filled_notional, o.total_fees, o.failure_code
FROM wallet_drawdown_order_operations AS op
JOIN wallet_drawdown_samples AS s USING (execution_account_id, slot_at)
LEFT JOIN execution_orders AS o USING (client_order_id, execution_account_id)
WHERE op.execution_account_id = 'main'
ORDER BY op.slot_at DESC, op.attempt;
```

### 回撤买单的延迟卖出

应用 `0050_wallet_drawdown_sell_links.sql` 后，使用独立的 `wallet-drawdown-sell.timer` 调用同一个二进制的 `--sell-due` 模式。定时器每次运行结束后等 30 秒再检查，到期时间按最后一笔确认买入成交时间加 5 分钟计算。可用 `--sell-due --dry-run` 查看本轮可挂出的卖单。

每个买单按全部已成交份额和实际买入成本计算统一目标价，买入手续费计入成本。限价向上对齐最小报价单位，使整笔买入对应的预计卖出金额减买入成本最接近且至少为 60 美元。报价须低于该批卖单计算时的卖一。没有合适报价时等待下一轮。卖出端手续费以实际账本记录为准。

每个开放持仓批次以全部剩余份额提交一笔 `SELL LIMIT GTC`。达到限价的买盘可以立即成交，未成交份额继续挂在原限价。每次提交前关联原买单和目标 lot 写入操作表。活动卖单及结果待确认的卖单由原执行链路处理，确定拒单后可在后续轮次重查盘口。份额精度超出交易所限制时由任务报告。

### 按回撤卖单报价提交公开买单

应用 `0051_wallet_drawdown_follow_buy_links.sql` 后，在卖出任务环境文件中将 `WALLET_DRAWDOWN_FOLLOW_BUY_ACCOUNT` 设为目标执行账户 ID，例如 `wallet-10`。该账户使用上面的 API 下单配置及 `wallet_drawdown` 风控授权。

任务每轮检查已进入 `LIVE` 或 `PARTIALLY_FILLED` 的回撤 GTC 卖单，用卖单限价和已入账的剩余份额提交一笔公开 `BUY LIMIT IOC`。交易所根据公开盘口撮合，未成交余量立即撤销。每笔买入先写操作表，用 `source_sell_client_order_id` 关联来源卖单。每笔来源卖单只触发一次买单尝试，提交结果不明确时沿用现有订单对账。通过 `--sell-due --dry-run` 可预览。

回退此配置时先停用任务侧的回撤交易与跟买，确认 API 专用账户的在途单已经收尾。切换服务配置时，同步停用对应的数据库下单授权，并从钱包文件移除只通过该配置接入的账户。保留账本和历史订单。

## HTTP、限流和错误

默认参数：单请求 5 秒超时、8 QPS、burst 4；均可注入配置。适配器没有对 POST/DELETE 自动重试。

本地令牌桶是**进程内、全部 CLOB 请求共用的一个桶**：下单、撤单、读订单与成交、心跳、对账、tick-size/neg-risk 读取都从这里取令牌。
速率与突发由 `POLYMARKET_REQUESTS_PER_SECOND` / `POLYMARKET_BURST` 配置，未设置时用上面的默认值。生产使用 20 / 10。

官方限额（`docs.polymarket.com/api-reference/trading-rate-limits`，Standard 档，按签名地址）：下单 40 个令牌/秒、突发 60；
撤单 80 个令牌/秒、突发 120。另有 Cloudflare 的 IP 级限额，例如 `/tick-size` 200 次/10 秒。实盘模式把本地配置上限卡在 40 / 60，
防止笔误让本地限速失去保护作用；官方档位调整时同步修改 `internal/config` 里的常量。

- `Kind=INVALID/REJECTED`：能够证明没有接受，可进入 `REJECTED`；
- `Kind=AMBIGUOUS`：请求可能已到达 CLOB，必须进入 `UNKNOWN`；
- `Kind=UNAVAILABLE`：只读调用暂时不可用；
- `Code`：鉴权、余额/allowance、精度、最小金额、FAK/FOK、rate limit、server error 等统一码。

签名完成后，适配器已知道 EIP-712 order hash。POST 超时时，这个预期 order ID 会随 `VenueError` 返回并持久化，reconciler 可以用它查询 `/data/order/{id}`，不需要再次下单。

## API 能力

已实现签名并提交 BUY/SELL、取消、单订单查询、分页 open orders、分页 trades/fills、tick size、neg risk，以及 `ORDER_STATUS_*` 和 placement `live/matched/delayed/unmatched` 的归一化。适配器兼容 CLOB raw API 的 bare-array 与分页 envelope，也兼容下单响应里的 raw `success/orderID/tradeIDs` 和 SDK 归一化 `ok/orderId/tradeIds` 字段。

POST 返回 `matched/delayed` 和 order 的累计 `size_matched` 都不能直接作为权威成交。适配器的
`FillSource` 只从 `/data/trades` 生成 taker 或 maker 分量；trade 明细尚未可见时继续保留预占并
轮询，不用限价、`makingAmount/takingAmount` 或 placement status 伪造成交。

CLOB 的 `MATCHED/size_matched` 只用于订单观察。资金、仓位和订单累计成交以真实 trade 与对应的
已确认 Polygon `OrderFilled` 证据为依据；当前权威账本只在 trade status 为 `CONFIRMED` 且证据
身份、数量和总费用全部吻合时入账。完整语义见
[`fills-and-position-ledger.md`](fills-and-position-ledger.md)。

官方对照：

- <https://docs.polymarket.com/api-reference/authentication>
- <https://docs.polymarket.com/trading/orders/create>
- <https://docs.polymarket.com/trading/orders/overview>
- <https://github.com/Polymarket/py-clob-client-v2>
