clean:
	go clean
	rm -Rf ./server ./vendor

test:
	go test ./...

installOutdatedMod:
	go install github.com/psampaz/go-mod-outdated@latest

outdatedDependencies: installOutdatedMod
	go list -u -m -json all | go-mod-outdated -direct
	go mod tidy
