
set -e

mkdir -p bin

cd bin

rm -f *.conf
rm -f data

ln -s ../data data
ln ../*.conf .

go build -o dp ../src/*.go
go build -gcflags "-N -l" -o ddp ../src/*.go

chmod +x dp
chmod +x ddp

#will need to add global settings -- maybe /var - not really keen on keeping credentials in there tho 
#sudo cp dp /usr/local/bin/
