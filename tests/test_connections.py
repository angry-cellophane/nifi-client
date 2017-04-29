from __future__ import print_function
from nifi import Nifi
import pytest
import time
import operator
import functools

pgs_res = {}
flow_res = {}
flow_id = {}
queues_res = {}

def setup_module(module):
    global pgs_res, flow_res, flow_id, queues_res
    nifi = Nifi('http://localhost:8080')
    pgs_res = nifi.resource('process-groups')
    flow_res = nifi.resource('flow')
    flow_id = flow_res.nifi_flow_id()
    queues_res = nifi.resource('flowfile-queues')

def test_list_and_empty_connections():
    pg = {
            'component': {
                'name': 'connections-test'
            },
        }

    created_pg = pgs_res.create(flow_id, pg)
    template = next(t for t in flow_res.list_templates() if t['template']['name'] == 'connections-test')
    template_instance = pgs_res.instantiate_template(created_pg['id'], {'templateId': template['id'], 'originX': 0, 'originY': 0})
    flow_res.start_pg(created_pg['id'])
    time.sleep(1)
    flow_res.stop_pg(created_pg['id'])
    connections = pgs_res.list_children(created_pg['id'], 'connections')
    assert len(connections) > 0

    flowfiles_count = functools.reduce(operator.add, [queues_res.list_requests(c['id'])['queueSize']['objectCount'] for c in connections])
    assert flowfiles_count > 0

    time.sleep(1)

    for connection in connections:
        queues_res.drop_requests(connection['id'])

    pgs_res.delete(created_pg)
