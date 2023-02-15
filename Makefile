.PHONY: default
default: create_volumes lint test

## Actions
.PHONY: create_volumes
create_volumes:
	docker volume create go-mod-cache
	docker volume create go-build-cache
	docker volume create go-lint-cache

.PHONY: lint
lint:
	golangci-lint run -v

.PHONY: test
test:
	go test -race -v ./...

.PHONY: test-integration
test-integration:
	docker-compose run --rm tests /bin/sh -c "go test -race -v -tags=integration ./..."

.PHONY: down
down:
	docker-compose down    
