.. Smart Transit Platform Geocoder documentation master file, created by
   sphinx-quickstart on Fri May  8 10:40:20 2015.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Smart Transit Platform Geocoder
===============================

Contents:

.. toctree::
   :maxdepth: 2

================================================ ===========
Endpoint                                         Description
================================================ ===========
/address/<city>/<streetname>                     Return all housenumbers with locations on the given street, in given city.
/address/<city>/<streetname>/<housenumber>       Return location for given housenumber in given street and city.
/suggest/<search term>[?city=City1[&city=City2]] Return all kinds of objects with given term anywhere in the name.
                                                 Streetnames are returned per each city and including number of addresses in the street.
                                                 All other objects include location.
                                                 Every type is in a separate list with a field name in result object.
                                                 Also returns streetnames with Levenshtein distance of two.
                                                 Query parameter city can be specified one or more times to limit suggestions to within those cities.
/reverse/<lat>,<lon>[?zoom=8]                    Return nearest address for given WGS84 coordinates.
                                                 If given zoom level is under 8, then returns municipality, not an address.
/interpolate/<streetname>/<housenumber>          Return an estimated location for an address interpolated from road network data.
/meta                                            Return metadata about the API.
                                                 Currently the date when the data was last updated in "updated" field in ISO 8601 format.
================================================ ===========

.. autotornado:: geocoder.app:app



Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

