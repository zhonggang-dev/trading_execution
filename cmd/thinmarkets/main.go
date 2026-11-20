// Command thinmarkets 只读地列出成交量与流动性都很小、且卖盘结构特殊的 Polymarket Outcome：
// 卖一全部吃掉的成本不超过上限，卖二与卖一的价差乘卖一份额超过下限。
// 它只读取 Gamma 与 CLOB 的公开接口，不需要钱包、数据库，也不会创建或修改任何订单。
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/adapter/polymarket"
	"github.com/UniPat-AI/trading_execution/internal/adapter/polymarketgamma"
	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/service/thinmarket"
)

type options struct {
	gammaURL  string
	clobURL   string
	workers   int
	batchSize int
	timeout   time.Duration
	params    thinmarket.Params
}

// main 启动命令并在失败时记录错误后退出。
func main() {
	parsed, err := parseOptions(os.Args[1:])
	if err != nil {
		slog.Error("thin market scan arguments are invalid", "error", err)
		os.Exit(2)
	}
	if err := run(parsed, os.Stdout); err != nil {
		slog.Error("thin market scan failed", "error", err)
		os.Exit(1)
	}
}

func parseOptions(arguments []string) (options, error) {
	var result options
	var maxVolume, maxLiquidity, minLiquidity, minGap, maxCost string
	flags := flag.NewFlagSet("thinmarkets", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	flags.StringVar(&result.gammaURL, "gamma-url", "https://gamma-api.polymarket.com", "Polymarket Gamma base URL")
	flags.StringVar(&result.clobURL, "clob-url", "https://clob.polymarket.com", "Polymarket CLOB base URL")
	flags.StringVar(&maxVolume, "max-volume", "", "market volume ceiling in USD (default 500)")
	flags.StringVar(&maxLiquidity, "max-liquidity", "", "market liquidity ceiling in USD (default 30000)")
	flags.StringVar(&minLiquidity, "min-liquidity", "", "market liquidity floor in USD; skips dormant zero-liquidity markets (default 0.01)")
	flags.StringVar(&minGap, "min-gap-value", "", "require (second ask - best ask) * best ask size > this USD value (default 60)")
	flags.StringVar(&maxCost, "max-best-ask-cost", "", "require best ask price * best ask size <= this USD value (default 10)")
	flags.IntVar(&result.params.Limit, "limit", 0, "maximum results (default 10)")
	flags.IntVar(&result.params.MaxScanMarkets, "max-scan-markets", 0, "maximum markets to inspect across all liquidity windows, at most 100000 (default 30000)")
	flags.IntVar(&result.params.ListConcurrency, "list-concurrency", 0, "concurrent Gamma list pages (default 4)")
	flags.IntVar(&result.workers, "book-workers", 4, "concurrent CLOB /books requests; keep low against production")
	flags.IntVar(&result.batchSize, "book-batch", 200, "tokens per CLOB /books request, at most 500")
	flags.DurationVar(&result.timeout, "timeout", 5*time.Minute, "overall deadline")
	if err := flags.Parse(arguments); err != nil {
		return options{}, err
	}
	if flags.NArg() != 0 {
		return options{}, fmt.Errorf("unexpected positional arguments")
	}
	result.params.MaxVolume = domain.Decimal(maxVolume)
	result.params.MaxLiquidity = domain.Decimal(maxLiquidity)
	result.params.MinLiquidity = domain.Decimal(minLiquidity)
	result.params.MinGapValue = domain.Decimal(minGap)
	result.params.MaxBestAskCost = domain.Decimal(maxCost)
	return result, nil
}

// run 执行一次筛选并把结果以 JSON 写到 output。
func run(options options, output io.Writer) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(ctx, options.timeout)
	defer cancel()

	httpClient := &http.Client{
		Timeout: 15 * time.Second,
		CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	markets, err := polymarketgamma.New(polymarketgamma.Params{
		BaseURL: options.gammaURL, HTTPClient: httpClient, UserAgent: "trading-execution-thinmarkets",
	})
	if err != nil {
		return err
	}
	books, err := polymarket.NewOrderBookSource(polymarket.OrderBookParams{
		BaseURL: options.clobURL, HTTPClient: httpClient, Workers: options.workers, BatchSize: options.batchSize,
	})
	if err != nil {
		return err
	}
	service, err := thinmarket.New(markets, books)
	if err != nil {
		return err
	}
	result, err := service.Find(ctx, options.params)
	if err != nil {
		return err
	}
	encoder := json.NewEncoder(output)
	encoder.SetIndent("", "  ")
	return encoder.Encode(result)
}
