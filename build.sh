
set -e

mkdir -p bin

cd bin

rm -f *.conf
rm -f data

ln -s ../data data &>/dev/null || :
ln ../*.conf . &>/dev/null || :

go build -o dp ../src/*.go
go build -gcflags "-N -l" -o ddp ../src/*.go

chmod +x dp
chmod +x ddp
