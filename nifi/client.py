import json
import requests

HEADERS = {'Content-Type': 'application/json'}
ALLOWED_RESOURCES = ['processors', 'connections', 'output-ports']


class RestResource:
    def __init__(self, url, session):
        self.__url = url
        self.__session = session

    def find(self, id):
        resp = self.__session.get('%s/%s' % (self.__url, id))
        if self.__is_ok(resp.status_code):
            return resp.json()
        elif self.__not_found(resp.status_code):
            return {}
        else:
            self.__exception(resp)

    def update(self, payload):
        resp = self.__session.put('%s/%s' % (self.__url, payload['id']), headers=HEADERS, data=json.dumps(payload))
        if self.__is_ok(resp.status_code):
            return resp.json()
        else:
            self.__exception(resp)

    def delete(self, id):
        resp = self.__session.delete('%s/%s' % (self.__url, id))
        if self.__is_ok(resp.status_code):
            return resp.json()
        else:
            self.__exception(resp)

    def __is_ok(self, status):
        return status >= 200 and status < 300

    def __not_found(self, status):
        return status >= 400 and status < 500

    def __exception(self, resp):
        raise Exception('Server returned exception: %s %s' % (resp.status_code, resp.text))




class Nifi:
    def __init__(self, url):
        self.__url = url
        self.__session = requests

    def resource(self, type):
        if type in ALLOWED_RESOURCES:
            return RestResource('%s/%s' % (self.__url, type), requests)
        raise Exception('Unsupported resource type %s' % type)
