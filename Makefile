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

coclient:
	docker exec -it cluster_cockroachdb_1_1 ./cockroach sql --insecure

ls:
	docker exec -it cluster_cockroachdb_2_1 ./cockroach node status --all --insecure --host cockroachdb_2
