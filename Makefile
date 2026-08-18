.PHONY: fmt test test-postgres vet run wallet-check check

fmt:
	go fmt ./...

test:
	go test ./...

test-postgres:
	@test -n "$(TRADING_EXECUTION_TEST_DATABASE_URL)" || (echo "TRADING_EXECUTION_TEST_DATABASE_URL is required" && exit 1)
	go test -run 'PostgresIntegration' -v ./internal/adapter/postgres

vet:
	go vet ./...

run:
	go run ./cmd/server

wallet-check:
	@test -n "$(POLYMARKET_ACCOUNTS_FILE)" || (echo "POLYMARKET_ACCOUNTS_FILE is required" && exit 1)
	go run ./cmd/walletcheck

check: fmt test vet
