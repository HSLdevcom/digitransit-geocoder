# -*- coding: utf-8 -*-
"""A setuptools based setup module.
See:
https://packaging.python.org/en/latest/distributing.html
"""

# Always prefer setuptools over distutils
from setuptools import setup
import sys
from os import path

here = path.abspath(path.dirname(__file__))

sphinx = ['sphinx', 'sphinxcontrib-httpdomain']

setup(
    name='geocoder',

    # Versions should comply with PEP440.  For a discussion on single-sourcing
    # the version across setup.py and the project code, see
    # https://packaging.python.org/en/latest/single_source_version.html
    version='0.1.0',

    description='Import tools from different datasources to ElasticSearch',

    # The project's main homepage.
    url='http://matka.hsl.fi',

    # Author details
    author='Tomi Pieviläinen',
    author_email='tomi.pievilainen@iki.fi',

    # Choose your license
    license='EUPL/AGPLv3',

    # See https://pypi.python.org/pypi?%3Aaction=list_classifiers
    classifiers=[
        # How mature is this project? Common values are
        #   3 - Alpha
        #   4 - Beta
        #   5 - Production/Stable
        'Development Status :: 2 - Pre-Alpha',

        # Indicate who your project is intended for
        'Intended Audience :: Developers',

        # Pick your license as you wish (should match "license" above)
        'License :: OSI Approved :: European Union Public Licence 1.1 (EUPL 1.1)',
        'License :: OSI Approved :: GNU Affero General Public License v3 or later (AGPLv3+)',

        # Specify the Python versions you support here. In particular, ensure
        # that you indicate whether you support Python 2, Python 3 or both.
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',

        'Topic :: Scientific/Engineering :: GIS',
        'Topic :: Software Development :: Pre-processors',
    ],

    # You can just specify the packages manually here if your project is
    # simple. Or you can use find_packages().
    # packages=find_packages(exclude=['contrib', 'docs', 'tests*']),
    packages=['geocoder'],

    # List run-time dependencies here.  These will be installed by pip when
    # your project is installed. For an analysis of "install_requires" vs pip's
    # requirements files see:
    # https://packaging.python.org/en/latest/requirements.html
    install_requires=[
        'pyelasticsearch',
        'GDAL',
        'pyproj',
        'click',
        'defusedxml',  # For National LandSurvey GML data
        'imposm.parser', 'rtree',  # For OpenStreetMap
        'ijson',  # For capital area service map
        'pyshp',  # For lipas
        'shapely',  # For NLS addresses
        'tornado', 'jinja2',  # For the web API
    ],

    setup_requires=sphinx,

    # List additional groups of dependencies here (e.g. development
    # dependencies). You can install these using the following syntax,
    # for example:
    # $ pip install -e .[dev,test]
    extras_require={
        # 'dev': ['check-manifest'],
        # 'test': ['coverage'],
    },

    # If there are data files included in your packages that need to be
    # installed, specify them here.  If using Python 2.6 or less, then these
    # have to be included in MANIFEST.in as well.
    package_data={
        # 'sample': ['package_data.dat'],
    },

    # To provide executable scripts, use entry points in preference to the
    # "scripts" keyword. Entry points provide cross-platform support and allow
    # pip to create the appropriate form of executable for the target platform.
    entry_points={
        'console_scripts': [
            'addresses=geocoder.addresses:main',
            'app=geocoder.app:main',
            'create_index=geocoder.create_index:main',
            'lipas=geocoder.lipas:main',
            'osm_pbf=geocoder.osm_pbf:main',
            'mml_addresses=geocoder.mml_addresses:main',
            'mml_municipalities=geocoder.mml_municipalities:main',
            'palvelukartta=geocoder.palvelukartta:main',
            'stops=geocoder.stops:main',
        ],
    },
)
