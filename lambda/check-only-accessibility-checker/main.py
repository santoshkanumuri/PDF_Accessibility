import json
import os

import boto3
from adobe.pdfservices.operation.auth.service_principal_credentials import ServicePrincipalCredentials
from adobe.pdfservices.operation.exception.exceptions import (
    SdkException,
    ServiceApiException,
    ServiceUsageException,
)
from adobe.pdfservices.operation.io.cloud_asset import CloudAsset
from adobe.pdfservices.operation.io.stream_asset import StreamAsset
from adobe.pdfservices.operation.pdf_services import ClientConfig, PDFServices
from adobe.pdfservices.operation.pdf_services_media_type import PDFServicesMediaType
from adobe.pdfservices.operation.pdfjobs.jobs.pdf_accessibility_checker_job import (
    PDFAccessibilityCheckerJob,
)
from adobe.pdfservices.operation.pdfjobs.result.pdf_accessibility_checker_result import (
    PDFAccessibilityCheckerResult,
)
from botocore.exceptions import ClientError

s3_client = boto3.client("s3")


def create_json_output_file_path():
    os.makedirs("/tmp/PDFAccessibilityChecker", exist_ok=True)
    return "/tmp/PDFAccessibilityChecker/result_before_remediation.json"


def get_secret(file_name):
    secret_name = "/myapp/client_credentials"
    session = boto3.session.Session()
    client = session.client(service_name="secretsmanager")

    try:
        secret_response = client.get_secret_value(SecretId=secret_name)
        secret = secret_response["SecretString"]
        secret_dict = json.loads(secret)
        client_id = secret_dict["client_credentials"]["PDF_SERVICES_CLIENT_ID"]
        client_secret = secret_dict["client_credentials"]["PDF_SERVICES_CLIENT_SECRET"]
        return client_id, client_secret
    except ClientError as err:
        print(f"Filename : {file_name} | Error: {err}")
        raise
    except KeyError as err:
        print(f"Filename : {file_name} | Missing secret key: {err}")
        raise


def build_response(status_code, body, origin):
    return {
        "statusCode": status_code,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": origin,
            "Access-Control-Allow-Methods": "POST,OPTIONS",
            "Access-Control-Allow-Headers": "Content-Type,Authorization,X-Amz-Date,X-Api-Key,X-Amz-Security-Token",
        },
        "body": json.dumps(body),
    }


def get_origin(event):
    allowed_origin = os.environ.get("ALLOWED_ORIGIN", "*")
    headers = event.get("headers") or {}
    request_origin = headers.get("origin") or headers.get("Origin")
    if allowed_origin == "*" and request_origin:
        return request_origin
    return allowed_origin


def parse_body(event):
    body = event.get("body")
    if isinstance(body, dict):
        return body
    if isinstance(body, str) and body.strip():
        return json.loads(body)
    return {}


def resolve_download_key(bucket_name, source_key):
    normalized_key = source_key.lstrip("/")
    file_name = os.path.basename(normalized_key)

    candidates = [normalized_key]
    if not normalized_key.startswith("pdf/"):
        candidates.append(f"pdf/{normalized_key}")
    if file_name and file_name not in candidates:
        candidates.append(file_name)
    if file_name and not file_name.startswith("pdf/"):
        candidates.append(f"pdf/{file_name}")

    tried = []
    for candidate in candidates:
        if candidate in tried:
            continue
        tried.append(candidate)
        try:
            s3_client.head_object(Bucket=bucket_name, Key=candidate)
            return candidate
        except ClientError:
            continue

    raise FileNotFoundError(
        f"Could not find input PDF in s3://{bucket_name}. Tried keys: {tried}"
    )


def save_report_to_s3(bucket_name, original_source_key, local_report_path, report_key_override):
    if report_key_override:
        target_key = report_key_override.lstrip("/")
    else:
        file_name = os.path.basename(original_source_key)
        file_key_without_extension = os.path.splitext(file_name)[0]
        target_key = (
            f"temp/{file_key_without_extension}/accessability-report/"
            f"{file_key_without_extension}_accessibility_report_before_remidiation.json"
        )

    with open(local_report_path, "rb") as data:
        s3_client.upload_fileobj(data, bucket_name, target_key)
    return target_key


def extract_score(report_json):
    if not isinstance(report_json, dict):
        return None

    summary = report_json.get("summary")
    if isinstance(summary, dict):
        for key in ("score", "accessibilityScore"):
            if key in summary:
                return summary.get(key)

    for key in ("score", "accessibilityScore"):
        if key in report_json:
            return report_json.get(key)

    return None


def lambda_handler(event, context):
    del context
    print("Received event:", event)
    origin = get_origin(event if isinstance(event, dict) else {})

    try:
        if not isinstance(event, dict):
            raise ValueError("Invalid event payload. Expected JSON object.")

        body = parse_body(event)
        bucket_name = (
            body.get("s3_bucket")
            or body.get("bucket")
            or os.environ.get("BUCKET_NAME")
        )
        source_key = (
            body.get("s3_key")
            or body.get("key")
            or body.get("file_key")
            or body.get("object_key")
            or body.get("fileName")
            or body.get("filename")
        )
        report_key_override = body.get("report_key") or body.get("reportKey")

        if not bucket_name:
            raise ValueError(
                "Missing bucket name. Provide 's3_bucket' or 'bucket', "
                "or set BUCKET_NAME in Lambda env."
            )
        if not source_key:
            raise ValueError(
                "Missing input file key. Provide one of: "
                "'s3_key', 'key', 'file_key', 'object_key', 'fileName', 'filename'."
            )

        download_key = resolve_download_key(bucket_name, source_key)
        local_pdf_path = f"/tmp/{os.path.basename(download_key)}"
        print(
            f"Downloading source PDF: s3://{bucket_name}/{download_key} -> {local_pdf_path}"
        )
        s3_client.download_file(bucket_name, download_key, local_pdf_path)

        with open(local_pdf_path, "rb") as pdf_file:
            input_stream = pdf_file.read()

        client_config = ClientConfig(connect_timeout=8000, read_timeout=40000)
        client_id, client_secret = get_secret(os.path.basename(download_key))
        credentials = ServicePrincipalCredentials(
            client_id=client_id, client_secret=client_secret
        )

        pdf_services = PDFServices(credentials=credentials, client_config=client_config)
        input_asset = pdf_services.upload(
            input_stream=input_stream, mime_type=PDFServicesMediaType.PDF
        )
        checker_job = PDFAccessibilityCheckerJob(input_asset=input_asset)
        location = pdf_services.submit(checker_job)
        job_result = pdf_services.get_job_result(location, PDFAccessibilityCheckerResult)

        report_asset: CloudAsset = job_result.get_result().get_report()
        stream_report: StreamAsset = pdf_services.get_content(report_asset)
        output_file_path_json = create_json_output_file_path()
        with open(output_file_path_json, "wb") as file:
            file.write(stream_report.get_input_stream())

        report_key = save_report_to_s3(
            bucket_name, source_key, output_file_path_json, report_key_override
        )
        print(f"Saved check-only accessibility report to s3://{bucket_name}/{report_key}")

        report_summary = {}
        score = None
        try:
            with open(output_file_path_json, "r", encoding="utf-8") as report_file:
                report_json = json.load(report_file)
                if isinstance(report_json, dict):
                    report_summary = report_json.get("summary") or {}
                    score = extract_score(report_json)
        except Exception as err:
            print(f"Could not parse report JSON for summary/score: {err}")

        return build_response(
            200,
            {
                "message": "Accessibility check completed.",
                "bucket": bucket_name,
                "input_key": download_key,
                "report_key": report_key,
                "score": score,
                "summary": report_summary,
            },
            origin,
        )
    except json.JSONDecodeError as err:
        return build_response(
            400, {"error": "Invalid JSON body", "details": str(err)}, origin
        )
    except ValueError as err:
        return build_response(400, {"error": str(err)}, origin)
    except FileNotFoundError as err:
        return build_response(404, {"error": str(err)}, origin)
    except (ServiceApiException, ServiceUsageException, SdkException) as err:
        print(f"Adobe PDF Services failure: {err}")
        return build_response(
            502,
            {"error": "Adobe PDF Services error", "details": str(err)},
            origin,
        )
    except Exception as err:
        print(f"Unhandled exception in check-only lambda: {err}")
        return build_response(
            500, {"error": "Internal server error", "details": str(err)}, origin
        )
