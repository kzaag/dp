
build:
	mkdir -p bin
	cd bin; rm -f *.conf;
	cd bin; rm -f data;
	cd bin; ln -s ../data data &>/dev/null || :
	cd bin; ln ../*.conf . &>/dev/null || :
	cd bin; go build -o dp ../src/*.go
	cd bin; go build -gcflags "-N -l" -o ddp ../src/*.go
	cd bin; chmod +x dp
	cd bin; chmod +x ddp

install: build
	sudo cp bin/dp /usr/local/bin/
