linters:
	go install github.com/golangci/golangci-lint/cmd/golangci-lint@v1.54.2
	golangci-lint -j 1 run --disable-all -E "errcheck" -E "godot" -E "govet" -E "ineffassign" -E "staticcheck" -E "unparam" -E "unused"
test: linters
	GOEXPERIMENT=loopvar go test -count 1 -coverprofile=coverage.out -shuffle on -short -v -dsn="postgres://postgres:everyone@localhost:5432/postgres?sslmode=disable" || (sleep 5; go test -coverprofile=coverage.out -shuffle on -short -v -dsn="postgres://postgres:everyone@localhost:5432/postgres?sslmode=disable")
	go tool cover -html=coverage.out -o coverage.html
