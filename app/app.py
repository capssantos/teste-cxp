
import csv
import io
import os
import zipfile
from datetime import datetime, timezone

import requests

from app.service.pipefy import Pipefy
from app.exception.exception import RegraNegocioException


pipefy = Pipefy()
ZIP_URL = "https://dados.cvm.gov.br/dados/FI/CAD/DADOS/registro_fundo_classe.zip"
DEFAULT_PIPE_ID = os.environ.get('PIPE_ID')


def _decode_csv_bytes(raw_bytes: bytes) -> str:
    try:
        return raw_bytes.decode("utf-8-sig")
    except UnicodeDecodeError:
        return raw_bytes.decode("latin-1")


def _normalize_value(value):
    if isinstance(value, str):
        return value.strip()
    return value


def _coerce_number(value):
    try:
        if isinstance(value, str):
            value = value.replace(".", "").replace(",", ".")
        return float(value)
    except Exception:
        return None


def _parse_in_values(value):
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        return [v.strip() for v in value.split(",") if v.strip()]
    return [value]


def _parse_between_values(value):
    if isinstance(value, (list, tuple)) and len(value) == 2:
        return value[0], value[1]
    if isinstance(value, str) and "," in value:
        parts = [p.strip() for p in value.split(",")]
        if len(parts) == 2:
            return parts[0], parts[1]
    raise RegraNegocioException("Filtro 'between' deve conter dois valores")


def _compare(op, field_value, filter_value):
    field_value = _normalize_value(field_value)
    filter_value = _normalize_value(filter_value)

    if op == "equals":
        return str(field_value).lower() == str(filter_value).lower()

    if op == "contains":
        return str(filter_value).lower() in str(field_value).lower()

    if op == "in":
        values = _parse_in_values(filter_value)
        return str(field_value).lower() in [str(v).lower() for v in values]

    if op in ["gt", "lt"]:
        fv_num = _coerce_number(field_value)
        flt_num = _coerce_number(filter_value)
        if fv_num is not None and flt_num is not None:
            return fv_num > flt_num if op == "gt" else fv_num < flt_num
        return str(field_value) > str(filter_value) if op == "gt" else str(field_value) < str(filter_value)

    if op == "between":
        start, end = _parse_between_values(filter_value)
        fv_num = _coerce_number(field_value)
        start_num = _coerce_number(start)
        end_num = _coerce_number(end)
        if fv_num is not None and start_num is not None and end_num is not None:
            return start_num <= fv_num <= end_num
        return str(start) <= str(field_value) <= str(end)

    raise RegraNegocioException(f"Operador inválido: {op}")


def _load_csv_from_zip(file_name: str):
    response = requests.get(ZIP_URL, timeout=60)
    if response.status_code != 200:
        raise RegraNegocioException(f"Falha ao baixar ZIP: {response.status_code}")

    with zipfile.ZipFile(io.BytesIO(response.content)) as zf:
        if file_name not in zf.namelist():
            raise RegraNegocioException(f"Arquivo '{file_name}' não encontrado no ZIP")

        with zf.open(file_name) as f:
            raw_bytes = f.read()
            text = _decode_csv_bytes(raw_bytes)
            return list(csv.DictReader(io.StringIO(text), delimiter=";"))


def _apply_filters(rows, filters: dict, operator: str):
    if not filters:
        return rows
    filtered = []
    for row in rows:
        ok = True
        for key, value in filters.items():
            if key == "operator":
                continue
            if key not in row:
                ok = False
                break
            if not _compare(operator, row.get(key), value):
                ok = False
                break
        if ok:
            filtered.append(row)
    return filtered


def run(requests, headers):
    if not isinstance(requests, dict):
        raise RegraNegocioException("Payload inválido")

    file_name = requests.get("file_name")
    filter_obj = requests.get("filter", {}) or {}

    if not file_name:
        raise RegraNegocioException("Campo 'file_name' é obrigatório")
    if not isinstance(filter_obj, dict):
        raise RegraNegocioException("Campo 'filter' deve ser um objeto JSON")

    operator = filter_obj.get("operator", "equals")
    filters = {k: v for k, v in filter_obj.items() if k != "operator"}

    rows = _load_csv_from_zip(file_name)
    matched = _apply_filters(rows, filters, operator)

    pipe_id = requests.get("pipe_id", DEFAULT_PIPE_ID)

    field_map = {
        "razao_social": "Denominacao_Social",
        "cnpj": "CNPJ_Fundo",
        "patrimonio_liquido": "Patrimonio_Liquido",
    }

    cards = []

    for dado in matched:
        fields_attributes = []
        for field_id, csv_key in field_map.items():
            fields_attributes.append(
                {"field_id": field_id, "field_value": dado.get(csv_key, "")}
            )

        response = pipefy.createCard(pipe_id, fields_attributes)
        if response.get("error") or response.get("errors"):
            raise RegraNegocioException(response.get("error") or response.get("errors"))

        card_id = response.get("id")
        if not card_id:
            raise RegraNegocioException("Falha ao criar card no Pipefy")

        created_at = datetime.now(timezone.utc).isoformat()
        cards.append({"id": card_id, "created_at": created_at})

    return {"cards": cards, "count": len(cards)}
