# TESTE CXP

Este projeto baixa o ZIP da CVM, lê o CSV dentro dele, aplica filtros e cria cards no Pipefy com os campos mapeados.

Fiz um pouco mais do que foi solicitado: além de filtrar, o fluxo já cria cards no Pipefy e retorna os IDs com a data/hora de criação.

## Requisitos

- Python 3.11+
- `pip`

## Instalação

1. Instale as dependências:

```bash
pip install -r requirements.txt
```

2. Configure o `.env` a partir do `.exemple.env`.

Campos importantes:

- `PIPEFY_TOKEN`: JSON com a service account do Pipefy.
- `PIPE_ID`: ID do Pipe que receberá os cards.

Exemplo:

```env
PIPEFY_TOKEN={"url_oauth_pipefy":"https://app.pipefy.com/oauth/token","client_id":"<seu_client_id>","client_secret":"<sua_client_secret>","grant_type":"client_credentials"}
PIPE_ID=13729
```

## Como rodar

Execute o servidor local pelo `main.py`:

```bash
python main.py
```

O serviço sobe em:

- `http://127.0.0.1:8090/main`

## Como chamar (Postman/Insomnia)

Faça um POST para:

```
http://127.0.0.1:8090/main
```

Payload de exemplo:

```json
{
  "file_name": "registro_fundo.csv",
  "filter": {
    "Tipo_Fundo": "FIDC",
    "Situacao": "Em Funcionamento Normal",
    "Gestor": "CATÁLISE INVESTIMENTOS LTDA.",
    "operator": "equals"
  }
}
```

## Sobre a criação do card

Para cada linha do CSV que passar pelo filtro, é criado um card no Pipefy.
Campos usados:

- `razao_social` ← `Denominacao_Social`
- `cnpj` ← `CNPJ_Fundo`
- `patrimonio_liquido` ← `Patrimonio_Liquido`

Além disso, o card é movido para a fase configurada no código (`phase_id` padrão).

