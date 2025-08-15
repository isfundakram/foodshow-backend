import json
import azure.functions as func
from ..common.storage import append_row, ATTENDANCE, now_iso

ATT_HEADERS = [
    "ts_iso","source","id_type","id_value","first_name","last_name",
    "company_name","registration_name","email","phone","attendee_type","status"
]

def main(req: func.HttpRequest, signalRMessages: func.Out[func.SignalRMessage]) -> func.HttpResponse:
    try:
        data = req.get_json()
    except:
        return func.HttpResponse("Invalid JSON", status_code=400)

    row = {
        "ts_iso": now_iso(),
        "source": "CHECKIN",
        "id_type": data.get("id_type","ACCT"),
        "id_value": data.get("id_value",""),
        "first_name": data.get("first_name",""),
        "last_name": data.get("last_name",""),
        "company_name": data.get("company_name",""),
        "registration_name": data.get("registration_name",""),
        "email": data.get("email",""),
        "phone": data.get("phone",""),
        "attendee_type": data.get("attendee_type","Customer"),
        "status": "IN_ATTENDANCE"
    }

    append_row(ATTENDANCE, ATT_HEADERS, row)

    # push to booth
    signalRMessages.set(func.SignalRMessage(
        target="inAttendance",
        arguments=[row]
    ))

    return func.HttpResponse(json.dumps({"ok": True}), mimetype="application/json")
