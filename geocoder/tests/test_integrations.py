from json import loads

import pytest
import requests


def test_address():
    r = requests.get('http://localhost:8888/address/Helsinki/Ida%20Aalbergin%20tie/9')
    assert r.status_code == 200
    results = loads(r.text)['results']
    assert len(results) == 1
    assert results[0] == {
        "municipalityFi": "Helsinki",
        "municipalitySv": "Helsingfors",
        "streetSv": "Ida Aalbergs väg",
        "streetFi": "Ida Aalbergin tie",
        "number": "9",
        "unit": None,
        "location": [24.896918441103022, 60.22986936848425],
        "source": "HRI.fi"
    }


def test_address_with_divisor_char():
    r = requests.get('http://localhost:8888/address/Helsinki/Opastinsilta/6a')
    assert r.status_code == 200
    results = loads(r.text)['results']
    assert len(results) == 1


def test_address_from_OSM():
    r = requests.get('http://localhost:8888/address/Helsinki/Vuorimiehenkatu/3')
    assert r.status_code == 200
    results = loads(r.text)['results']
    assert len(results) == 1
    assert results[0] == {
        "municipalityFi": "Helsinki",
        "municipalitySv": "Helsinki",
        "streetSv": "Vuorimiehenkatu",
        "streetFi": "Vuorimiehenkatu",
        "number": "3",
        "unit": None,
        'location': [24.955355600000015, 60.160892999999994],
        "source": "OSM"
    }


def test_non_latin_address():
    r = requests.get('http://localhost:8888/address/Helsinki/Ida%20Aalbergs%20v%c3%a4g/9')
    assert r.status_code == 200
    results = loads(r.text)['results']
    assert len(results) == 1
    assert results[0] == {
        "municipalityFi": "Helsinki",
        "municipalitySv": "Helsingfors",
        "streetSv": "Ida Aalbergs väg",
        "streetFi": "Ida Aalbergin tie",
        "number": "9",
        "unit": None,
        "location": [24.896918441103022, 60.22986936848425],
        "source": "HRI.fi"
    }


def test_not_existing_address():
    r = requests.get('http://localhost:8888/address/Helsinki/Mannerheimintie/9999')
    assert r.status_code == 404


def test_street():
    r = requests.get('http://localhost:8888/street/Helsinki/Mannerheimintie')
    assert r.status_code == 200
    results = loads(r.text)['results']
    assert len(results) == 161


def test_street_order():
    r = requests.get('http://localhost:8888/street/Helsinki/Opastinsilta')
    assert r.status_code == 200
    results = loads(r.text)['results']
    assert ', '.join((results[0]['number'],
                      results[1]['number'],
                      results[7]['number'],
                      results[8]['number'],
                      results[9]['number'])) == '1, 2, 8, 8a, 8b'


def test_not_existing_street():
    r = requests.get('http://localhost:8888/street/Helsinki/Foo%20Bar%20road')
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
    assert loads(r.text) == {'coordinates': [24.943123528522143, 60.16661631427898]}


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


def test_suggest_streetname_with_filter():
    # Incomplete name should match from middle
    r = requests.get('http://localhost:8888/suggest/ietoti?city=Espoo')
    assert r.status_code == 200
    results = loads(r.text)
    assert len(results['streetnames_fi']) == 1
    assert len(results['streetnames_fi'][0]['Tietotie']) == 1
    assert results['streetnames_fi'][0]['Tietotie'][0] == \
        {"key": "espoo", "doc_count": 6}


def test_suggest_swedish_streetname_with_filter():
    # Incomplete name should match from middle
    r = requests.get('http://localhost:8888/suggest/Datav?city=Espoo')
    assert r.status_code == 200
    results = loads(r.text)
    assert len(results['streetnames_fi']) == 0
    assert len(results['streetnames_sv']) == 1
    assert len(results['streetnames_sv'][0]['Datavägen']) == 1
    assert results['streetnames_sv'][0]['Datavägen'][0] == \
        {"key": "esbo", "doc_count": 6}


def test_suggest_streetname_with_filter_2():
    # Incomplete name should match from middle
    r = requests.get('http://localhost:8888/suggest/ietoti?city=Espoo&city=Vantaa')
    assert r.status_code == 200
    results = loads(r.text)
    assert len(results['streetnames_fi']) == 1
    assert len(results['streetnames_fi'][0]['Tietotie']) == 2


def test_suggest_streetname_with_filter_3():
    # Incomplete name should match from middle
    r = requests.get('http://localhost:8888/suggest/ietoti?city=Helsinki')
    assert r.status_code == 200
    results = loads(r.text)
    assert len(results['streetnames_fi']) == 0


def test_suggest_stop_name():
    # WeeGee culture house should appear only in two stop names,
    # and in no stop descriptions
    r = requests.get('http://localhost:8888/suggest/weegee')
    assert r.status_code == 200
    assert len(loads(r.text)['stops']) == 2


def test_suggest_stop_name_with_filter():
    r = requests.get('http://localhost:8888/suggest/Pohjantie')
    assert r.status_code == 200
    assert len(loads(r.text)['stops']) == 178
    r = requests.get('http://localhost:8888/suggest/Pohjantie?city=Parikkala')
    assert r.status_code == 200
    assert len(loads(r.text)['stops']) == 4


# Currently Digiroad data doesn't have descriptions
@pytest.mark.xfail
def test_suggest_stop_desc():
    # Pohjantie should only appear in descriptions of stops WeeGee and Kaskenkaataja,
    # not in names
    r = requests.get('http://localhost:8888/suggest/Itsehallintotie')
    assert r.status_code == 200
    assert len(loads(r.text)['stops']) == 1


def test_suggest_stop_code():
    r = requests.get('http://localhost:8888/suggest/E1971')
    assert r.status_code == 200
    results = loads(r.text)
    assert len(results['stops']) == 1
    s = results['stops'][0]
    # Result might contain extra data, but verify what is specified
    assert s["nameFi"] == "Majurinkulma"
    assert s["nameSv"] == "Majorshörnet"
    assert s["stopCode"] == "E1971"
    assert s["municipalityFi"] == "Espoo"


@pytest.mark.xfail
def test_suggest_missing_data():
    # Test fields that should be eventually populated,
    # but are known to be missing at this point
    r = requests.get('http://localhost:8888/suggest/E1971')
    s = loads(r.text)['stops'][0]
    assert s["stopDesc"] == "Itsehallintotie"
    assert s["address"] == "Majurinkulma 2"


def test_suggest_fuzzy_typo_fix():
    r = requests.get('http://localhost:8888/suggest/Mannehreimintie')
    assert r.status_code == 200
    results = loads(r.text)
    assert len(results['fuzzy_streetnames']) == 1
