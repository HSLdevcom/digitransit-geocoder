FROM debian:testing
RUN \
    apt-get update && \
    apt-get install -y curl wget && \
    wget -qO - https://packages.elasticsearch.org/GPG-KEY-elasticsearch | apt-key add - && \
    echo "deb http://packages.elasticsearch.org/elasticsearch/1.4/debian stable main" > /etc/apt/sources.list.d/elasticsearch.list && \
    apt-get update && \
    apt-get install -y default-jre elasticsearch \
        git mercurial \
        libprotobuf-dev protobuf-compiler libspatialindex-dev \
        python3-gdal python3-dev python3-pip \
        unzip && \
    echo "discovery.zen.ping.multicast.enabled: false" >> /etc/elasticsearch/elasticsearch.yml

WORKDIR /app

COPY setup.py requirements.txt /app/
RUN pip3 install -r requirements.txt

COPY geocoder /app/geocoder/
COPY docs /app/docs/
RUN pip3 install .

RUN hg clone https://bitbucket.org/birkenfeld/sphinx-contrib && \
    pip3 install sphinx-contrib/httpdomain && \
    cd docs && \
    make html

COPY scripts /app/scripts/
RUN scripts/import-data.sh

CMD ["scripts/run-server.sh", "8888"]

EXPOSE 8888
