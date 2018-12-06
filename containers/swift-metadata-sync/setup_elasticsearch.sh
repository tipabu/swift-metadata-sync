#!/bin/bash

set -e

cd /elasticsearch-${ES_VERSION}

if [ -e ./bin/elasticsearch-certutil ]; then
    ./bin/elasticsearch-certutil cert --silent --pem --in /tmp/instances.yml --out ./config/certs.zip
else
    ./bin/elasticsearch-plugin install x-pack
    ./bin/x-pack/certgen --in /tmp/instances.yml --out ./config/certs.zip
fi

cd ./config
ls -al elasticsearch.yml
sed -i "s/<VERSION>/${ES_VERSION}/g" elasticsearch.yml
ls -al elasticsearch.yml
unzip certs.zip
