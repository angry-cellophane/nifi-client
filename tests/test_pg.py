from __future__ import print_function
from nifi import Nifi
import pytest

pgs_res = {}
flow_res = {}
flow_id = {}


def setup_module(module):
    global pgs_res, flow_res, flow_id
    nifi = Nifi('http://localhost:8080')
    pgs_res = nifi.resource('process-groups')
    flow_res = nifi.resource('flow')
    flow_id = flow_res.nifi_flow_id()


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
        }

    created_pg = pgs_res.create(flow_id, pg)
    assert 'id' in created_pg

    found_pg = pgs_res.find(created_pg['id'])
    assert found_pg['id'] == created_pg['id']
    assert found_pg['component']['name'] == created_pg['component']['name']

    revision = found_pg['revision']
    pgs_res.delete(created_pg)
    found_pg = pgs_res.find(created_pg['id'])
    assert not found_pg


def test_create_child_with_not_allowed_type():
    with pytest.raises(Exception):
        pgs_res.create_child('nonexistent type', 'pg_id', {})


def test_create_child():
    pg = {
            'component': {
                'name': 'test'
            },
         }

    created_pg = pgs_res.create(flow_id, pg)
    assert 'id' in created_pg
    assert 'version' in created_pg['revision']

    try:
        processor = {
                'component': {
                    'name': 'test',
                    'type': 'org.apache.nifi.processors.standard.PutFile'
                }
        }
        pgs_res.create_child(created_pg['id'], 'processors', processor)
    finally:
        pgs_res.delete(created_pg)


def test_list_children():
    pg = {
            'component': {
                'name': 'test'
            },
         }

    created_pg = pgs_res.create(flow_id, pg)
    assert 'id' in created_pg
    assert 'version' in created_pg['revision']

    try:
        processor = {
                'component': {
                    'name': 'test',
                    'type': 'org.apache.nifi.processors.standard.PutFile'
                }
        }

        pg_id = created_pg['id']
        pgs_res.create_child(pg_id, 'processors', processor)
        pgs_res.create_child(pg_id, 'processors', processor)
        processors = pgs_res.list_children(pg_id, 'processors')
        assert len(processors) == 2
    finally:
        pgs_res.delete(created_pg)

def test_instantiate_template():
    templates = flow_res.list_templates()
    assert len(templates) > 0

    template = templates[0]

    pg = {
            'component': {
                'name': 'test templates'
            },
         }

    created_pg = pgs_res.create(flow_id, pg)

    created_template = pgs_res.instantiate_template(created_pg['id'], {'templateId': template['id'], 'originX': 0.0, 'originY': 0.0})
    pgs_res.delete(created_pg)
