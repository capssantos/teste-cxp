import os, uuid

from app.app import run
from sys import platform
from flask import Flask, request, jsonify
from app.exception.exception import RegraNegocioException
from dotenv import load_dotenv
load_dotenv()

app = Flask(__name__)

def process(request_process):
    code = 200
    message = 'success'
    header = {}
    response_data = None

    try:
        _header = {}

        if request_process.headers.get('Webhook-Retry') is not None and \
           request_process.headers.get('Webhook-Retry').lower() == 'true':
            print(f"headers: {list(request_process.headers)}")
            print(f"request: {request_process.json}")
            raise RegraNegocioException("Requisição já processada")

        response_data = run(request_process.get_json(), request_process.headers)

    except RegraNegocioException as e:
        _header = {}
        print(e)
    except Exception as ex:
        _header = {}
        code = 500
        message = str(ex)

    header.update(_header)
    response = {
        'function': {
            'name': os.environ.get('PYTHON_NAME'),
            'version': os.environ.get('PYTHON_VERSION'),
        },
        'status': {'code': code, 'message': message}
    }
    if response_data not in ['', None, []]:
        # response_data é dict -> OK
        response['data'] = response_data

    print(f"response: {response}")
    return jsonify(response), response['status']['code'], header



# Region Metodo para a Google Cloud Plataform
def main(request_main):
    return process(request_main)


# End Region

# Region Metodo para Testar Local via Postman/Rest Client
# url:http://127.0.0.1:8090/main


@app.route('/main', methods=['POST'])
def main_flask():
    return process(request)


if __name__ == "__main__":
    if platform in ["win32", "win64"]:
        app.run(host='0.0.0.0', port=8090, debug=True)


# End Region