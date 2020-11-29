
build:
	mkdir -p bin
	go build -o ./bin/dp
	cd bin; ln -sf ./../example/pg .
	cd bin; ln -sf ./../example/cass .

dbg: build
	go build -gcflags "-N -l" -o ../bin/dp

install: build
	sudo cp bin/dp /usr/local/bin/;
	go install;

cass: build
	cd bin; ./dp -c cass/conf.yml -v

casse: build
	cd bin; ./dp -c cass/conf.yml -v -e
