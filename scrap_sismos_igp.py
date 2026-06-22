import json
import os
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone


IGP_REPORTS_PAGE = "https://ultimosismo.igp.gob.pe/productos/reportes-sismicos"
DEFAULT_REPORTS_ENDPOINT = "https://ultimosismo.igp.gob.pe/api/ultimo-sismo/ajaxb"
DEFAULT_TABLE_NAME = "UltimosSismosIGP"
LIMA_TIMEZONE = timezone(timedelta(hours=-5))


def lambda_handler(event, context):
    table_name = os.environ.get("IGP_SISMOS_TABLE", DEFAULT_TABLE_NAME)
    reports_endpoint = os.environ.get("IGP_REPORTS_ENDPOINT", DEFAULT_REPORTS_ENDPOINT)

    try:
        reports = get_latest_reports(reports_endpoint, limit=10)
        sync_dynamodb(table_name, reports)

        return build_response(
            200,
            {
                "message": "Se almacenaron los 10 últimos sismos reportados por el IGP.",
                "source_page": IGP_REPORTS_PAGE,
                "total": len(reports),
                "items": reports,
            },
        )
    except urllib.error.HTTPError as error:
        return build_response(
            error.code,
            {
                "message": "No se pudo obtener la información del IGP.",
                "error": str(error),
            },
        )
    except urllib.error.URLError as error:
        return build_response(
            502,
            {
                "message": "Error de conexión al consultar el IGP.",
                "error": str(error.reason),
            },
        )
    except Exception as error:
        return build_response(
            500,
            {
                "message": "Ocurrió un error al procesar los reportes sísmicos.",
                "error": str(error),
            },
        )


def get_latest_reports(base_endpoint, limit=10):
    current_year = datetime.now(LIMA_TIMEZONE).year
    reports = fetch_reports_by_year(base_endpoint, current_year)

    if len(reports) < limit:
        previous_year_reports = fetch_reports_by_year(base_endpoint, current_year - 1)
        reports.extend(previous_year_reports)

    reports = [normalize_report(report) for report in reports if report.get("codigo")]
    reports.sort(key=sort_report_key, reverse=True)
    return reports[:limit]


def fetch_reports_by_year(base_endpoint, year):
    url = f"{base_endpoint.rstrip('/')}/{year}"
    request = urllib.request.Request(
        url,
        headers={
            "User-Agent": "api-sismos-igp-serverless/1.0",
            "Accept": "application/json",
        },
    )

    with urllib.request.urlopen(request, timeout=20) as response:
        payload = response.read().decode("utf-8")

    data = json.loads(payload)
    if not isinstance(data, list):
        raise ValueError("La respuesta del IGP no tiene el formato esperado.")

    return data


def normalize_report(report):
    return {
        "codigo": text_value(report.get("codigo")),
        "idlistasismos": report.get("idlistasismos"),
        "fecha_local": text_value(report.get("fecha_local")),
        "hora_local": text_value(report.get("hora_local")),
        "fecha_utc": text_value(report.get("fecha_utc")),
        "hora_utc": text_value(report.get("hora_utc")),
        "latitud": text_value(report.get("latitud")),
        "longitud": text_value(report.get("longitud")),
        "magnitud": text_value(report.get("magnitud")),
        "profundidad": report.get("profundidad"),
        "referencia": text_value(report.get("referencia")),
        "intensidad": text_value(report.get("intensidad")),
        "numero_reporte": report.get("numero_reporte"),
        "reporte_acelerometrico_pdf": text_value(report.get("reporte_acelerometrico_pdf")),
        "createdAt": text_value(report.get("createdAt")),
        "updatedAt": text_value(report.get("updatedAt")),
        "source_page": IGP_REPORTS_PAGE,
        "scrapedAt": datetime.now(timezone.utc).isoformat(),
    }


def text_value(value):
    return "" if value is None else str(value)


def sort_report_key(report):
    created_at = report.get("createdAt") or ""
    codigo = report.get("codigo") or ""
    numero_reporte = report.get("numero_reporte") or 0
    return created_at, codigo, numero_reporte


def sync_dynamodb(table_name, reports):
    import boto3

    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table(table_name)
    latest_codes = {report["codigo"] for report in reports}

    with table.batch_writer() as batch:
        for item in scan_all_items(table):
            if item.get("codigo") not in latest_codes:
                batch.delete_item(Key={"codigo": item["codigo"]})

        for report in reports:
            batch.put_item(Item=report)


def scan_all_items(table):
    scan_kwargs = {}

    while True:
        response = table.scan(**scan_kwargs)

        for item in response.get("Items", []):
            yield item

        last_key = response.get("LastEvaluatedKey")
        if not last_key:
            break

        scan_kwargs["ExclusiveStartKey"] = last_key


def build_response(status_code, body):
    return {
        "statusCode": status_code,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
        },
        "body": json.dumps(body, ensure_ascii=False),
    }
