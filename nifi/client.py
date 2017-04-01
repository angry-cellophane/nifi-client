import json
import requests
import urllib

HEADERS = {'Content-Type': 'application/json'}
ALLOWED_RESOURCES = ['processors', 'connections', 'output-ports']


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

    def delete(self, id, version, client_id={}):
        query = {'version': version}
        if client_id:
            query['clientId'] = client_id

        url = '%s/%s?%s' % (self._url, id, urllib.urlencode(query))
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


class ProcessGroup(RestResource):
    def __init__(self, url, session):
        RestResource.__init__(self, '%s/process-groups' % (url), session)

    def create(self, parent_id, pg):
        url = '%s/%s/process-groups' % (self._url, parent_id)
        resp = self._session.post(url, headers=HEADERS, data=json.dumps(pg))
        if not self._is_ok(resp.status_code):
            self._throw_exc(resp)

        return resp.json()


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

    def list_pg(self):
        res = self.__search('')
        return res['processGroupResults']

    def nifi_flow_id(self):
        pgs = self.list_pg() # doens't contain flow id, but at least one of the processors has groupId = flow id
        group_ids = [pg['groupId'] for pg in pgs]
        for id in group_ids:
            group = self.__pgs.find(id)
            if group['component']['name'] == 'NiFi Flow':
                return group['id']

        raise Exception('Cannot find flow id')


class Nifi:
    def __init__(self, url):
        self.__url = url
        self.__session = requests

    def resource(self, rawtype):
        type = rawtype.lower()

        if type == 'flow':
            process_groups = RestResource('%s/%s' % (self.__url, 'process-groups'), self.__session)
            return Flow(self.__url, self.__session, process_groups)
        elif type == 'process-groups':
            return ProcessGroup(self.__url, self.__session)
        elif type in ALLOWED_RESOURCES:
            return RestResource('%s/%s' % (self.__url, type), self.__session)

        raise Exception('Unsupported resource type %s' % type)