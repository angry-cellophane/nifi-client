from __future__ import print_function
from nifi import Nifi


pgs_res = {}
flow_res = {}


def setup_module(module):
    global pgs_res, flow_res
    nifi = Nifi('http://localhost:8080/nifi-api')
    pgs_res = nifi.resource('process-groups')
    flow_res = nifi.resource('flow')


def test_find_pg():
    pgs = flow_res.list_pg()
    assert len(pgs) > 0
    assert 'id' in pgs[0]
    pg = pgs_res.find(pgs[0]['id'])
    assert 'id' in pg


def test_create_and_delete_pg():
    pg = {
            'component': {
                'name': 'test'
            },
            'revision': {
                'version': 0,
            }}

    flow_id = flow_res.nifi_flow_id()

    created_pg = pgs_res.create(flow_id, pg)
    assert 'id' in created_pg

    found_pg = pgs_res.find(created_pg['id'])
    assert found_pg['id'] == created_pg['id']
    assert found_pg['component']['name'] == created_pg['component']['name']

    revision = found_pg['revision']
    print(found_pg)
    pgs_res.delete(created_pg['id'], revision['version'])
    found_pg = pgs_res.find(created_pg['id'])
    assert not found_pg
