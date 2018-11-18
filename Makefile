install:
	dep ensure

test:
	go test ./{mysql,tests}

testv:
	go test -v ./{mysql,tests}
