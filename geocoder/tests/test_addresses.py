# -*- coding: utf-8 -*-
from geocoder import addresses
import io


def test_csv(monkeypatch):
    monkeypatch.setattr("geocoder.utils.ES.index_op", lambda x: x)

    result = addresses.documents(io.StringIO('''katunimi,osoitenumero,osoitenumero2,kiinteiston_jakokirjain,kaupunki,yhdistekentta,N,E,gatan,staden,tyyppi,tyyppi_selite,ajo_pvm
Adjutantinpolku,2,,,Helsinki,Adjutantinpolku 2 Helsinki,6674867,25500025,Adjutantstigen,Helsingfors,1,osoite tai katu,2015-01-13
Virsutie,4,6,,Vantaa,Virsutie 4-6 Vantaa,6689909,25504051,Näverskovägen,Vanda,1,osoite tai katu,2015-01-13
                                           ''')).__next__()
    expected = {
        'katunimi': 'Adjutantinpolku',
        'osoitenumero': 2,
        'osoitenumero2': '',
        'kiinteiston_jakokirjain': '',
        'kaupunki': 'Helsinki',
        'location': (25.000450568957046, 60.18663906268862),
        'gatan': 'Adjutantstigen',
        'staden': 'Helsingfors'}

    assert result == expected
