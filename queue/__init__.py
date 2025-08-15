import json
import azure.functions as func
from ..common.storage import queue_snapshot

def main(req: func.HttpRequest) -> func.HttpResponse:
    return func.HttpResponse(json.dumps(queue_snapshot()), mimetype="application/json")
