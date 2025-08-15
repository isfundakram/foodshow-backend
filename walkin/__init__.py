import json
import azure.functions as func
from ..common.storage import append_row, WALKINS, ATTENDANCE, now_iso, make_walkin_id

W_HEADERS = ["ts_iso","type","customer_acct","first_name","last_name","company_name","email","phone","created_by"]
ATT_HEADERS = [
    "ts_iso","source","id_type","id_value","first_name","last_name",
    "company_name","registration_name","email","phone","attendee_type","status"
]

def main(req: func.HttpRequest, signalRMessages: func.Out[func.SignalRMessage]) -> func.HttpResponse:
    try:
        data = req.get_json()
    except:
        return func.HttpResponse("Invalid JSON", status_code=400)

    attendee_type = data.get("type","Customer")
    walkin_id = make_walkin_id()

    # log in walkins.csv
    walkin_row = {
        "ts_iso": now_iso(),
        "type": attendee_type,
        "customer_acct": data.get("customer_acct",""),
        "first_name": data.get("first_name",""),
        "last_name": data.get("last_name",""),
        "company_name": data.get("company_name",""),
        "email": data.get("email",""),
        "phone": data.get("phone",""),
        "created_by": data.get("created_by","SELF")
    }
    append_row(WALKINS, W_HEADERS, walkin_row)

    # also enqueue into attendance as IN_ATTENDANCE so booth sees it
    attendance_row = {
        "ts_iso": walkin_row["ts_iso"],
        "source": "WALKIN",
        "id_type": "WALKIN",
        "id_value": walkin_id,
        "first_name": walkin_row["first_name"],
        "last_name": walkin_row["last_name"],
        "company_name": walkin_row["company_name"],
        "registration_name": f"{walkin_row['first_name']} {walkin_row['last_name']}".strip(),
        "email": walkin_row["email"],
        "phone": walkin_row["phone"],
        "attendee_type": attendee_type,
        "status": "IN_ATTENDANCE"
    }
    append_row(ATTENDANCE, ATT_HEADERS, attendance_row)

    # broadcast to booth
    signalRMessages.set(func.SignalRMessage(
        target="inAttendance",
        arguments=[attendance_row]
    ))

    return func.HttpResponse(json.dumps({"ok": True, "walkin_id": walkin_id}), mimetype="application/json")
