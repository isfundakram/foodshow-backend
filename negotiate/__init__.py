import json
import azure.functions as func

def main(req: func.HttpRequest, connectionInfo) -> func.HttpResponse:
    return func.HttpResponse(
        json.dumps({"url": connectionInfo["url"], "accessToken": connectionInfo["accessToken"]}),
        mimetype="application/json"
    )
