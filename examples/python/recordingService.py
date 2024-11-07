import os
import uuid
import requests
from requests import RequestException
from services.service_discovery import get_internal_service_uri
from utils.purecloud_context import PC_CONTEXT
from utils.exceptions import InternalError
from utils.log import LOG
import json
from typing import Dict
SERVICE_NAME = 'RECORDING'
RECORDING_QUERY_URI = "/recordings/v2/organizations/{org_id}/recordingIds/queries"
ENVIRONMENT = os.environ.get('ENVIRONMENT', 'dev')

def check_recording_service(org_id, session_ids) -> Dict:
    root_uri = get_internal_service_uri(SERVICE_NAME)
    request_uri = root_uri + RECORDING_QUERY_URI.format(org_id=org_id)
    try:
        PC_CONTEXT.set_organization(org_id)
        PC_CONTEXT.set_correlation_id(str(uuid.uuid4()))
        PC_CONTEXT._add_entry('Accept', 'application/json')
        PC_CONTEXT._add_entry('Content-Type', 'application/json')
        response = requests.post(url=request_uri, data=json.dumps(session_ids), headers=PC_CONTEXT.get_copy())
    except RequestException as ex:
        msg = f"Failed to get recording status for {session_ids} - {ex}"
        LOG.error(message=msg, request_uri=request_uri)
        raise InternalError(message=msg, url=request_uri)
    if response.ok:
        response_json_str = response.json()
        return response_json_str
    else:
        if response.status_code == 404:
            msg = "Recording service has returned HTTP404 for the current request"
            LOG.error(message=msg)
            return None
        else:
            LOG.error(message="Failed to get recording status",
                      status_code=response.status_code, request_uri=request_uri)
            response.raise_for_status()