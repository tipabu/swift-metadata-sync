FROM bouncestorage/swift-aio:latest

RUN apt-get update &&\
    apt-get install --no-install-recommends -y wget openjdk-8-jre-headless locales git unzip &&\
    apt-get clean &&\
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt requirements-test.txt /tmp/
RUN pip install -e git://github.com/swiftstack/container-crawler.git@0.1.1#egg=container-crawler && \
    pip install -r /tmp/requirements-test.txt -r /tmp/requirements.txt && \
    pip uninstall -y hacking

RUN locale-gen en_US.UTF-8
ENV LANG='en_US.UTF-8' LANGUAGE='en_US:en' LC_ALL='en_US.UTF-8'

RUN useradd -M -d /tmp elastic
ARG es_version=6.5.1
RUN wget --progress=bar:force:noscroll https://artifacts.elastic.co/downloads/elasticsearch/elasticsearch-${es_version}.tar.gz &&\
    tar -xvf elasticsearch-${es_version}.tar.gz
COPY containers/swift-metadata-sync/setup_elasticsearch.sh containers/swift-metadata-sync/instances.yml /tmp/
COPY containers/swift-metadata-sync/elasticsearch.yml /elasticsearch-${es_version}/config/
RUN chown -R elastic elasticsearch-${es_version}

ENV ES_VERSION=${es_version}
RUN sudo -E -u elastic /tmp/setup_elasticsearch.sh

EXPOSE 9200
CMD ["/bin/bash", "/swift-metadata-sync/containers/swift-metadata-sync/launch.sh"]
