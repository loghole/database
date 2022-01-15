.PHONY: lint
lint:
	golangci-lint run -v

.PHONY: test
test:
	go test -race -v ./...

.PHONY: test-integration
test-integration:
	docker-compose run --rm tests /bin/sh -c "go test -race -v -tags=integration ./..."
