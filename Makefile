install:
	dep ensure

test:
	go test -v github.com/localhots/bocadillo/tests
