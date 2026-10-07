GOLANGCI_LINT := go run -mod=readonly github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.14.0

linters:
	$(GOLANGCI_LINT) run --default=none \
		-E "errcheck" \
		-E "errname" \
		-E "errorlint" \
		-E "exhaustive" \
		-E "gocritic" \
		-E "godot" \
		-E "govet" \
		-E "grouper" \
		-E "ineffassign" \
		-E "ireturn" \
		-E "misspell" \
		-E "prealloc" \
		-E "predeclared" \
		-E "revive" \
		-E "staticcheck" \
		-E "thelper" \
		-E "unparam" \
		-E "unused" \
		./...
	$(GOLANGCI_LINT) fmt --diff \
		-E "gci" \
		-E "gofmt" \
		-E "goimports"
test: linters
	go test -count 1 -coverprofile=coverage.out -shuffle on -short -v -dsn="postgres://postgres:everyone@localhost:5432/postgres?sslmode=disable" || (sleep 5; go test -coverprofile=coverage.out -shuffle on -short -v -dsn="postgres://postgres:everyone@localhost:5432/postgres?sslmode=disable")
	go tool cover -html=coverage.out -o coverage.html
