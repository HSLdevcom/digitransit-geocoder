#!/bin/sh
# Create a named container for persistent data using a really simple image
# as a base so that the container won't be recreated when our full image updates
docker build -t geocoder .
docker run --name geocoding_data -v /data busybox true
