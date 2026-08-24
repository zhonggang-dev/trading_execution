.PHONY: fmt test test-deploy test-postgres vet run wallet-check wallet-approve-dry-run check

fmt:
	go fmt ./...

test:
	go test ./...

test-deploy:
	python3 -m unittest discover -s deploy -p 'test_*.py'

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

wallet-approve-dry-run:
	@test -n "$(POLYMARKET_ACCOUNTS_FILE)" || (echo "POLYMARKET_ACCOUNTS_FILE is required" && exit 1)
	@test -n "$(POLYGON_RPC_URL)" || (echo "POLYGON_RPC_URL is required" && exit 1)
	go run ./cmd/walletapprove

check: fmt test test-deploy vet
