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
- size 必须不低于 `/book.min_order_size`；
- BUY notional 默认不得低于 `1 pUSD`，可配置但不能为零；
- FAK/FOK BUY 的 maker notional 最多 4 位小数；SELL 的 taker notional 保持最多 4 位小数；
- 除上述明确的 BUY 整数化外，不合法的策略数量直接拒绝；Go 不会为了通过交易所校验而二次改小 shares 或改变方向。

旧 `execute.py` 用 float 先算 USD、再除以价格、再交给 SDK 二次 round-down，可能把 `16.90` 变成 `16.89`。新实现直接生成 6-decimal 整数，例如 `16.90 shares → 16900000`，彻底移除双重舍入。

原生支持 `GTC/GTD/FAK/FOK`。Polymarket 没有 IOC 类型，adapter 通过 `SupportsTimeInForce(IOC)=false`
声明这一点，执行层据此模拟 IOC：订单以 `GTC` 限价签名并提交，数量为 Market Validation 记录的
`executable_size`（最新盘口在 `worst_price` 内可成交的量，不超过策略 `size`，不低于 `min_order_size`），
下单响应返回后执行层立刻撤掉未成交部分。IOC 绝不会被改成 FAK：FAK/FOK BUY 的 maker notional 是 pUSD 预算，
盘口好于限价时会买到多于 `size` 的 shares；GTC 按股数成交，最多买 `size` 股，价差体现为少花钱。

## HTTP、限流和错误

默认参数：单请求 5 秒超时、8 QPS、burst 4；均可注入配置。适配器没有对 POST/DELETE 自动重试。

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
