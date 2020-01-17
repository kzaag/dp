
set -e

mkdir -p bin

cd bin

rm -f *.conf
rm -f data

ln -s ../data data
ln ../*.conf .

go build -o dp ../src/*.go
chmod +x dp

#will need to add global settings -- maybe /var - not really keen on keeping credentials in there tho 
#sudo cp dp /usr/local/bin/
