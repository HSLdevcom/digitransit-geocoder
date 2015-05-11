from json import loads

import requests


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


def test_suggest_streetname():
    # Incomplete name should match from middle
    r = requests.get('http://localhost:8888/suggest/ietoti')
    assert r.status_code == 200
    results = loads(r.text)
    assert len(results['streetnames_fi']) == 1
    assert len(results['streetnames_fi'][0]['Tietotie']) == 2
    # asserting lists takes order into account, but we don't care about it
    # so we just check that both results are in the list
    assert {"key": "vantaa", "doc_count": 10} \
        in results['streetnames_fi'][0]['Tietotie']
    assert {"key": "espoo", "doc_count": 6} \
        in results['streetnames_fi'][0]['Tietotie']


def test_suggest_stop_name():
    # WeeGee culture house should appear only in two stop names,
    # and in no stop descriptions
    r = requests.get('http://localhost:8888/suggest/weegee')
    assert r.status_code == 200
    assert len(loads(r.text)['stops']) == 2


def test_suggest_stop_desc():
    # Pohjantie should only appear in descriptions of stops WeeGee and Kaskenkaataja,
    # not in names
    r = requests.get('http://localhost:8888/suggest/Pohjantie')
    assert r.status_code == 200
    assert len(loads(r.text)['stops']) == 4


def test_suggest_stop_code():
    r = requests.get('http://localhost:8888/suggest/E1971')
    assert r.status_code == 200
    results = loads(r.text)
    assert len(results['stops']) == 1
    assert results['stops'][0] == {
        "stop_url": "http://aikataulut.hsl.fi/pysakit/fi/2118206.html",
        "location_type": "0",
        "location": [24.826333500000132, 60.21038490000018],
        "stop_id": "2118206",
        "parent_station": " ",
        "stop_desc": "Itsehallintotie",
        "stop_name": "Majurinkulma",
        "zone_id": "2",
        "wheelchair_boarding": "0",
        "stop_code": "E1971"
    }


def test_suggest_fuzzy_typo_fix():
    r = requests.get('http://localhost:8888/suggest/Mannehreimintie')
    assert r.status_code == 200
    results = loads(r.text)
    assert len(results['fuzzy_streetnames']) == 1
