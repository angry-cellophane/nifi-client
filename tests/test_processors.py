from __future__ import print_function
from nifi import Nifi
import requests


processor_id = {}
nifi = {}
resource = {}


def setup_module(module):
    resp = requests.get('http://localhost:8080/nifi-api/flow/search-results?q=')
    global processor_id, nifi, resource
    processor_id = resp.json()['searchResultsDTO']['processorResults'][0]['id']
    nifi = Nifi('http://localhost:8080')
    resource = nifi.resource('processors')


def test_find():
    processor = resource.find(processor_id)
    assert 'id' in processor
    assert 'component' in processor
    assert 'revision' in processor
