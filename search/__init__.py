import json
import azure.functions as func
from ..common.storage import search_registered

def main(req: func.HttpRequest) -> func.HttpResponse:
    q = req.params.get("q", "")
    results = search_registered(q, limit=50)
    return func.HttpResponse(json.dumps(results), mimetype="application/json")
