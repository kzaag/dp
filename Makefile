
build:
	mkdir -p bin
	cd src; go build -o ../bin/dp

dbg: build
	cd src; go build -gcflags "-N -l" -o ../bin/dp

install: build
	sudo cp bin/dp /usr/local/bin/
