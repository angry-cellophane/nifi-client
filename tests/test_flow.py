from __future__ import print_function
from nifi import Nifi


resource = {}


def setup_module(module):
    global resource
    nifi = Nifi('http://localhost:8080/nifi-api')
    resource = nifi.resource('flow')


def test_list_pg():
    pgs = resource.list_pg()
    assert len(pgs) > 0
    assert 'id' in pgs[0]


def test_start_pg():
    pg = resource.list_pg()[0]
    print(pg['id'])
    assert 'id' in pg
    resource.start_pg(pg['id'])


def test_stop_pg():
    pg = resource.list_pg()[0]
    print(pg['id'])
    assert 'id' in pg
    resource.stop_pg(pg['id'])


def test_find_flow_id():
    id = resource.nifi_flow_id()
    assert id.strip()
