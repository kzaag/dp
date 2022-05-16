
build:
	mkdir -p bin
	go build -o ./bin/dp

dbg: build
	go build -gcflags "-N -l" -o ../bin/dp

install: build
	sudo cp bin/dp /usr/local/bin/;
	go install;
