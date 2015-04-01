FROM debian:testing
RUN \
    apt-get update && \
    apt-get install -y curl wget && \
    wget -qO - https://packages.elasticsearch.org/GPG-KEY-elasticsearch | apt-key add - && \
    echo "deb http://packages.elasticsearch.org/elasticsearch/1.4/debian stable main" > /etc/apt/sources.list.d/elasticsearch.list && \
    apt-get update && \
    apt-get install -y default-jre elasticsearch \
        python3-gdal python3-dev python3-pip \
        git libprotobuf-dev protobuf-compiler

WORKDIR /app

COPY setup.py requirements.txt /app/

RUN pip3 install . -r requirements.txt

COPY addresses.py mml.py osm_pbf.py palvelukartta.py docker.sh /app/

COPY geocoding-data /app/geocoding-data/

RUN ["sh", "docker.sh"]

COPY app.py run-server.sh /app/

ENTRYPOINT ["/bin/sh", "run-server.sh"]

EXPOSE 8888
