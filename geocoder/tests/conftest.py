import subprocess
from time import sleep

import pytest


@pytest.fixture(scope='module')
def app(request):
    p = subprocess.Popen(['app', '-d' '2015-01-01'])
    sleep(1)

    def fin():
        p.terminate()

    request.addfinalizer(fin)
    return p  # provide the fixture value
