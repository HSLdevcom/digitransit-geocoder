#!/bin/sh
docker run -ti --rm -P --volumes-from geocoding_data geocoder
