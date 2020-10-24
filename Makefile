
build:
	mkdir -p bin
	cd src; go build -o ../bin/dp
	cd bin; ln -sf ./../example/pg .

dbg: build
	cd src; go build -gcflags "-N -l" -o ../bin/dp

install: build
	sudo cp bin/dp /usr/local/bin/
