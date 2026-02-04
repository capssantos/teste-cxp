import requests, json, re, os, base64, tempfile
from time import sleep
from datetime import datetime
from urllib.parse import unquote
from app.exception.exception import PipefyException
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


class Pipefy(object):
    """ Integration class with Pipefy rest api. """

    def __init__(self):
        """ altered by: silvio.angelo@accenture.com  """
        pipefy_credencials = json.loads(os.getenv('PIPEFY_TOKEN'))
        self.qtdTentativasReconexao = int(os.environ.get('PFY_QTD_TENTATIVAS_RECONEXAO'))
        self.timeoutConexao = int(os.environ.get('PFY_TIMEOUT_CONEXAO'))
        self.base_url = os.environ.get('PFY_BASE_URL')
        self.api_endpoint = self.base_url + os.environ.get('PFY_API_URL')
        self.sa_oauth_endpoint = pipefy_credencials.get('url_oauth_pipefy')

        self.pat_token = os.environ.get('PFY_PAT_TOKEN')
        self.sa_client_id = pipefy_credencials.get('client_id')
        self.sa_secret = pipefy_credencials.get('client_secret')

        self.verify_ssl = True if os.environ.get('REQUESTS_SSL').lower() in ['true'] else False

        session_request = requests.Session()
        retry = Retry(total=5, backoff_factor=45)
        adapter = HTTPAdapter(max_retries=retry)
        session_request.mount("https://", adapter)
        self.session_request = session_request
        self.tmp = tempfile.gettempdir()

        self.jwt_pipefy_token = self.get_pipefy_jwt()

        self.token = self.pat_token if self.jwt_pipefy_token in ['', None, []] else self.jwt_pipefy_token

        if self.token in ['', None, []]:
            raise PipefyException("Pipefy Authorization ")

        self.headers = {'Content-Type': 'application/json', 'Authorization': f'Bearer {self.token}'}

    def get_pipefy_jwt(self):
        print(f'INICIO Recuperando Autenticação JWT Pipefy')
        for i in range(self.qtdTentativasReconexao):
            try:
                headers = {'Content-Type': 'application/json'}
                payload_oauth = {
                    "grant_type":"client_credentials",
                    "client_id": self.sa_client_id,
                    "client_secret": self.sa_secret
                }

                response = self.session_request.post(
                    self.sa_oauth_endpoint,
                    json=payload_oauth,
                    headers=headers,
                    verify=self.verify_ssl,
                    timeout=self.timeoutConexao
                )
                try:
                    status_code = response.status_code
                    response = json.loads(response.text)
                except ValueError:
                    print(f"response: {response.text}")
                    raise PipefyException(response.text)

                print(f"response: {response}")
                if status_code != requests.codes.ok:
                    print(f"response: {response.get('error')}")
                    err_desc = (
                        response.get("error_description")
                        or response.get("error")
                        or str(response)
                    )
                    raise PipefyException(f"Pipefy API error: {err_desc}")

                if "DOCTYPE html" in response:
                    print(f"response: {response}")
                    raise PipefyException("Error Http 429 - Too Many Requests")

                return response.get('access_token')
            except Exception as e:
                print(f"Tentativa: {i} Error Message: {e}")
                print(f"Aguardando {self.timeoutConexao} seg para realizar nova tentativa")
                sleep(self.timeoutConexao)
                if i > self.qtdTentativasReconexao-1:
                    raise e
        print(f'FIM Recuperando Autenticação JWT Pipefy')

    def request(self, query, headers={}):
        """ altered by: silvio.angelo@accenture.com  """
        print(f"query: {query}")

        for i in range(self.qtdTentativasReconexao):
            try:
                _headers = self.headers
                _headers.update(headers)
                response = self.session_request.post(
                    self.api_endpoint,
                    json={"query": query},
                    headers=_headers,
                    verify=self.verify_ssl,
                    timeout=self.timeoutConexao
                )
                try:
                    status_code = response.status_code
                    response = json.loads(response.text)
                except ValueError:
                    print(f"response: {response.text}")
                    raise PipefyException(response.text)

                print(f"response: {response}")
                if status_code != requests.codes.ok:
                    print(f"response: {response.get('error')}")
                    err_desc = (
                        response.get("error_description")
                        or response.get("error")
                        or str(response)
                    )
                    raise PipefyException(f"Pipefy API error: {err_desc}")

                if "DOCTYPE html" in response:
                    print(f"response: {response}")
                    raise PipefyException("Error Http 429 - Too Many Requests")

                return response
            except Exception as e:
                print(f"Tentativa: {i} Error Message: {e}")
                print(f"Aguardando {self.timeoutConexao} seg para realizar nova tentativa")
                sleep(self.timeoutConexao)
                if i > self.qtdTentativasReconexao-1:
                    raise e

    def __prepare_json_dict(self, data_dict):
        data_response = json.dumps(data_dict)
        rex = re.compile(r'"(\S+)":')
        for field in rex.findall(data_response):
            data_response = data_response.replace('"%s"' % field, field)
        return data_response


    def createCard(self, pipe_id, fields_attributes, parent_ids=[], response_fields=None, headers={}):
        """ Create card: Mutation to create a card, in case of success a query is returned. """

        response_fields = response_fields or 'card { id title }'
        query = '''
            mutation {
              createCard(
                input: {
                  pipe_id: %(pipe_id)s
                  fields_attributes: %(fields_attributes)s
                  parent_ids: [ %(parent_ids)s ]
                }
              ) { %(response_fields)s }
            }
        ''' % {
            'pipe_id': json.dumps(pipe_id),
            'fields_attributes': self.__prepare_json_dict(fields_attributes),
            'parent_ids': ', '.join([json.dumps(id) for id in parent_ids]),
            'response_fields': response_fields,
        }
        response = self.request(query, headers)

        if response.get('error'):
            return {'error': response.get('error')}
        elif response.get('errors'):
            return {'errors': response.get('errors')}
        else:
            return response.get('data', {}).get('createCard', {}).get('card')

