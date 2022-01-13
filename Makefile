GOFLAGS = CGO_ENABLED=0 GOOS=linux GOARCH=amd64
GOTEST_PACKAGES = $(shell go list ./... | egrep -v '(pkg|cmd)')

test:
	go test -race -v -cover -coverprofile coverage.out $(GOTEST_PACKAGES)

lint:
	golangci-lint run -v

test-intergation:
	cd tests/reconnect && docker-compose up

test-intergation-stop:
	cd tests/reconnect && docker-compose down

test-retry:
	cd tests/retry && docker-compose up -d postgres
	cd tests/retry && docker-compose up test

test-retry-stop:
	cd tests/retry && docker-compose down
