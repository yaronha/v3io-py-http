
import requests
import base64
import json

class v3io:
    def __init__(self, address, user, password, container):
        self.url = "http://" + address + "/"
        self.auth = (user, password)
        if container != None and container != "" :
            self.container = container + '/'
        else :
            self.container = ""

    def _getheader(self, command):
        return { "Content-Type": "application/json", "X-v3io-function": command }

    def putrecords(self, path, messages):
        records = []

        for msg in messages:
            data = base64.b64encode(bytes(msg, 'utf-8')).decode('utf-8')
            records += [ {"Data" :data}]

        payload = { "Records": records }
        headers = self._getheader('PutRecords')
        if not path.endswith('/'):
            path += '/'

        return requests.post(self.url +self.container +path, headers=headers, auth=self.auth, data=json.dumps(payload))

    def updateitem(self, path, key, expr, condition):
        payload = { "UpdateExpression": expr, "ConditionExpression" : condition }
        headers = self._getheader('UpdateItem')
        path += '/' + key

        return requests.post(self.url +self.container +path, headers=headers, auth=self.auth, data=json.dumps(payload))




#v3 = v3io('<IP>:8081','<user>','<pass>', 'bigdata')
#v3.putrecords('mystream', ['msg1']).text
