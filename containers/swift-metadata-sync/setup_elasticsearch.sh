#!/bin/bash

set -e

es_configs=( ${ES_VERSION} ${PORT} ${OLD_ES_VERSION} ${OLD_PORT} )

for i in `seq 0 1`; do
    version=${es_configs[$(( $i * 2 ))]}
    port=${es_configs[$(( ($i * 2) + 1))]}

    cd /elasticsearch-${version}

    if [ -e ./bin/elasticsearch-certutil ]; then
        ./bin/elasticsearch-certutil cert --silent --pem --in /tmp/instances.yml --out ./config/certs.zip
    else
        ./bin/elasticsearch-plugin install x-pack
        ./bin/x-pack/certgen --in /tmp/instances.yml --out ./config/certs.zip
    fi

    cd ./config
    sed -i "s/<VERSION>/${version}/g" elasticsearch.yml
    sed -i "s/<PORT>/${port}/g" elasticsearch.yml
    unzip certs.zip
done
