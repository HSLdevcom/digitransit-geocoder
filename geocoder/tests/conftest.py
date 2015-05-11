import subprocess
from time import sleep

import pytest


@pytest.yield_fixture(scope='session', autouse=True)
def app():
    p = subprocess.Popen(['app', '-d' '2015-01-01'])
    sleep(1)
    yield p
    p.terminate()
