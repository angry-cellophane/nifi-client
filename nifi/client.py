import json
import requests
import urllib

HEADERS = {'Content-Type': 'application/json'}
ALLOWED_RESOURCES = ['processors', 'connections', 'output-ports', 'process-groups']


class RestResource:
    def __init__(self, url, session):
        self._url = url
        self._session = session

    def find(self, id):
        resp = self._session.get('%s/%s' % (self._url, id))
        if self._is_ok(resp.status_code):
            return resp.json()
        elif self._is_not_found(resp.status_code):
            return {}
        else:
            self._throw_exc(resp)

    def update(self, payload):
        resp = self._session.put('%s/%s' % (self._url, payload['id']), headers=HEADERS, data=json.dumps(payload))
        if self._is_ok(resp.status_code):
            return resp.json()
        else:
            self._throw_exc(resp)

    def delete(self, resource):
        if 'revision' not in resource:
            raise Exception('No revision found in %s. Please provide {"revision": {"version": <<version_value>>} }' % (resource))

        revision = resource['revision']
        query = {'version': revision['version']}
        if 'clientId' in revision:
            query['clientId'] = resource['revision']['clientId']

        url = '%s/%s?%s' % (self._url, resource['id'], urllib.urlencode(query))
        resp = self._session.delete(url)
        if self._is_ok(resp.status_code):
            return resp.json()
        else:
            self._throw_exc(resp)

    def _is_ok(self, status):
        return status >= 200 and status < 300

    def _is_not_found(self, status):
        return status >= 400 and status < 500

    def _throw_exc(self, resp):
        raise Exception('Server returned exception: %s %s' % (resp.status_code, resp.text))

class FlowFileQueue(RestResource):
    def __init__(self, url, session):
        RestResource.__init__(self, '%s/flowfile-queues' % (url), session)

    def drop_requests(self, queue_id):
        resp = self._session.post('%s/%s/drop-requests' % (self._url, queue_id))
        if not self._is_ok(resp.status_code):
            self._throw_exc(resp)

        return resp.json()['dropRequest']

    def list_requests(self, queue_id):
        resp = self._session.post('%s/%s/listing-requests' % (self._url, queue_id))
        if not self._is_ok(resp.status_code):
            self._throw_exc(resp)

        return resp.json()['listingRequest']

class ProcessGroup(RestResource):
    def __init__(self, url, session):
        RestResource.__init__(self, '%s/process-groups' % (url), session)

    def create(self, parent_id, pg):
        pg = self.__init_version(pg)
        url = '%s/%s/process-groups' % (self._url, parent_id)
        resp = self._session.post(url, headers=HEADERS, data=json.dumps(pg))
        if not self._is_ok(resp.status_code):
            self._throw_exc(resp)

        return resp.json()

    def create_child(self, pg_id, resource_type, processor):
        processor = self.__init_version(processor)
        self.__check_res_type(pg_id, resource_type)

        url = '%s/%s/processors' % (self._url, pg_id)
        resp = self._session.post(url, headers=HEADERS, data=json.dumps(processor))
        if not self._is_ok(resp.status_code):
            self._throw_exc(resp)

        return resp.json()

    def list_children(self, pg_id, resource_type):
        self.__check_res_type(pg_id, resource_type)
        url = '%s/%s/%s' % (self._url, pg_id, resource_type)
        resp = self._session.get(url)
        if not self._is_ok(resp.status_code):
            self._throw_exc(resp)

#       nifi returns a result set that contains an arrays of child objects.
#       The key of the arrays in the set is not the same as the resource name (e.g. output-ports vs outputPorts)
#       Instead of keeping a map of resource and key names using this dirty hack to take a first value
        result_set = resp.json()
        set_key = result_set.keys()[0]
        return result_set[set_key]

    def __init_version(self, obj):
        if 'revision' not in obj:
            obj['revision'] = {
                'version': 0
            }
        elif 'version' not in obj:
            obj['revision']['version'] = 0

        return obj

    def __check_res_type(self, pg_id, res_type):
        if res_type not in ALLOWED_RESOURCES:
            raise Exception('Creation a resource of type %s for the process group %s are not allowed, only the following type: %s' % (res_type, pg_id, ALLOWED_RESOURCES))

    def instantiate_template(self, pg_id, template):
        url = '%s/%s/template-instance' % (self._url, pg_id)
        resp = self._session.post(url, headers=HEADERS, data=json.dumps(template))
        if not self._is_ok(resp.status_code):
            self._throw_exc(resp)

        return resp.json()['flow']

class Flow(RestResource):
    def __init__(self, url, session, process_groups):
        RestResource.__init__(self, '%s/flow' % (url), session)
        self.__pgs = process_groups

    def start_pg(self, pg_id):
        self.__set_pg_state(pg_id, 'RUNNING')

    def stop_pg(self, pg_id):
        self.__set_pg_state(pg_id, 'STOPPED')

    def __set_pg_state(self, pg_id, state):
        url = '%s/process-groups/%s' % (self._url, pg_id)
        data = {'id': pg_id, 'state': state}
        resp = self._session.put(url, headers=HEADERS, data=json.dumps(data))
        if not self._is_ok(resp.status_code):
            self._throw_exc(resp)

    def __search(self, query):
        url = '%s/search-results?q=%s' % (self._url, query)
        resp = self._session.get(url)
        if not self._is_ok(resp.status_code):
            self._throw_exc(resp)

        return resp.json()['searchResultsDTO']

    def list_pgs(self):
        res = self.__search('')
        return res['processGroupResults']

    def nifi_flow_id(self):
        pgs = self.list_pgs() # doens't contain flow id, but at least one of the processors has groupId = flow id
        group_ids = [pg['groupId'] for pg in pgs]
        for id in group_ids:
            group = self.__pgs.find(id)
            if group['component']['name'] == 'NiFi Flow':
                return group['id']

        raise Exception('Cannot find flow id')

    def list_templates(self):
        url = '%s/templates' % (self._url)
        resp = self._session.get(url)
        return resp.json()['templates']

class Nifi:
    def __init__(self, url):
        self.__url = '%s/nifi-api' % (url)
        self.__session = requests

    def resource(self, rawtype):
        type = rawtype.lower()

        if type == 'flow':
            process_groups = RestResource('%s/%s' % (self.__url, 'process-groups'), self.__session)
            return Flow(self.__url, self.__session, process_groups)
        elif type == 'process-groups':
            return ProcessGroup(self.__url, self.__session)
        elif type == 'flowfile-queues':
            return FlowFileQueue(self.__url, self.__session)
        elif type in ALLOWED_RESOURCES:
            return RestResource('%s/%s' % (self.__url, type), self.__session)

        raise Exception('Unsupported resource type %s' % type)
