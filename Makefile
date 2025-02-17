linters:
	GOTOOLCHAIN=go1.24.0 go run -mod=readonly github.com/golangci/golangci-lint/cmd/golangci-lint@latest run --disable-all \
		-E "errcheck" \
		-E "errname" \
		-E "errorlint" \
		-E "exhaustive" \
		-E "gci" \
		-E "gocritic" \
		-E "godot" \
		-E "gofmt" \
		-E "goimports" \
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
test: linters
	go test -count 1 -coverprofile=coverage.out -shuffle on -short -v -dsn="postgres://postgres:everyone@localhost:5432/postgres?sslmode=disable" || (sleep 5; go test -coverprofile=coverage.out -shuffle on -short -v -dsn="postgres://postgres:everyone@localhost:5432/postgres?sslmode=disable")
	go tool cover -html=coverage.out -o coverage.html
