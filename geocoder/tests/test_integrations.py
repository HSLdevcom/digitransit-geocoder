from json import loads

import pytest
import requests


pytestmark = pytest.mark.usefixtures("app")


def test_address():
    r = requests.get('http://localhost:8888/search/Helsinki/Ida%20Aalbergin%20tie/9')
    assert r.status_code == 200
    results = loads(r.text)['results']
    assert len(results) == 1
    assert results[0] == {
        "municipality-fi": "Helsinki",
        "municipality-sv": "Helsingfors",
        "street-sv": "Ida Aalbergs väg",
        "street-fi": "Ida Aalbergin tie",
        "number": "9",
        "unit": None,
        "location": [24.896918441103022, 60.22986936848425],
        "source": "HRI.fi"
    }


def test_non_latin_address():
    r = requests.get('http://localhost:8888/search/Helsinki/Ida%20Aalbergs%20v%c3%a4g/9')
    assert r.status_code == 200
    results = loads(r.text)['results']
    assert len(results) == 1
    assert results[0] == {
        "municipality-fi": "Helsinki",
        "municipality-sv": "Helsingfors",
        "street-sv": "Ida Aalbergs väg",
        "street-fi": "Ida Aalbergin tie",
        "number": "9",
        "unit": None,
        "location": [24.896918441103022, 60.22986936848425],
        "source": "HRI.fi"
    }


def test_not_existing_address():
    r = requests.get('http://localhost:8888/search/Helsinki/Mannerheimintie/9999')
    assert r.status_code == 404


def test_street():
    r = requests.get('http://localhost:8888/search/Helsinki/Ida%20Aalbergin%20tie')
    assert r.status_code == 200
    results = loads(r.text)['results']
    assert len(results) == 11


def test_not_existing_street():
    r = requests.get('http://localhost:8888/search/Helsinki/Foo%20Bar%20road')
    assert r.status_code == 404


def test_reverse_city():
    r = requests.get('http://localhost:8888/reverse/60.1841593,24.9494081?city')
    #  The city includes boundaries data which is quite long, so just check the name
    assert loads(r.text)['nimi'] == 'Helsinki'


def test_reverse_address():
    r = requests.get('http://localhost:8888/reverse/60.1841593,24.9494081')
    assert loads(r.text) == {
        "katunimi": "Läntinen Papinkatu",
        "osoitenumero": 1,
        "staden": "Helsingfors",
        "location": [24.949359889140478, 60.184143202636754],
        "gatan": "Västra Prästgatan",
        "kiinteiston_jakokirjain": "",
        "kaupunki": "Helsinki",
        "osoitenumero2": 1
    }


def test_not_existing_interpolate():
    r = requests.get('http://localhost:8888/interpolate/Mannerheimintie/9999')
    assert r.status_code == 404


def test_interpolate():
    r = requests.get('http://localhost:8888/interpolate/Mannerheimintie/2')
    assert r.status_code == 200
    assert loads(r.text) == {'coordinates': [0, 0]}


def test_meta():
    r = requests.get('http://localhost:8888/meta')
    assert loads(r.text) == {'updated': '2015-01-01'}
