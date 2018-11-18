install:
	dep ensure

test:
	go test -v ./{mysql,tests}
