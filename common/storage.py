import os, io, csv, datetime, re
from typing import List, Dict
from azure.storage.blob import BlobServiceClient, ContainerClient, BlobClient, AppendBlobClient

CONTAINER = os.environ.get("BLOB_CONTAINER", "foodshow")
ROOT_REGISTERED = "registered/registered.csv"
ATTENDANCE = "attendance/attendance.csv"
WALKINS = "walkins/walkins.csv"

def _svc() -> BlobServiceClient:
    return BlobServiceClient.from_connection_string(os.environ["AzureWebJobsStorage"])

def _container_client() -> ContainerClient:
    cli = _svc().get_container_client(CONTAINER)
    if not cli.exists():
        cli.create_container()
    return cli

def _append_blob_client(path: str) -> AppendBlobClient:
    cli = _svc().get_blob_client(CONTAINER, path)
    return AppendBlobClient(cli.url, credential=_svc().credential)

def _ensure_append_with_header(path: str, header_fields: List[str]):
    cc = _container_client()
    bc = cc.get_blob_client(path)
    if not bc.exists():
        abc = _append_blob_client(path)
        abc.create_append_blob()
        header_line = _csv_line(header_fields, {h: h for h in header_fields}, header_only=True)
        abc.append_block(header_line.encode("utf-8"))

def _csv_line(headers: List[str], row: Dict[str, str], header_only=False) -> str:
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=headers, extrasaction="ignore")
    if header_only:
        # Write header by hand to keep field order
        buf.write(",".join(headers) + "\n")
    else:
        writer.writerow({h: (row.get(h, "") or "") for h in headers})
    return buf.getvalue()

def append_row(path: str, headers: List[str], row: Dict[str, str]):
    _ensure_append_with_header(path, headers)
    abc = _append_blob_client(path)
    data = _csv_line(headers, row)
    abc.append_block(data.encode("utf-8"))

def read_registered_rows() -> List[Dict[str, str]]:
    cc = _container_client()
    bc: BlobClient = cc.get_blob_client(ROOT_REGISTERED)
    if not bc.exists():
        return []
    text = bc.download_blob().readall().decode("utf-8", errors="ignore")
    f = io.StringIO(text)
    dr = csv.DictReader(f)
    rows = []
    for r in dr:
        # normalize keys present in header
        rows.append({
            "customer_acct": (r.get("customer_acct","") or "").strip(),
            "first_name": (r.get("first_name","") or "").strip(),
            "last_name": (r.get("last_name","") or "").strip(),
            "company_name": (r.get("company_name","") or "").strip(),
            "registration_name": (r.get("registration_name","") or "").strip(),
            "email": (r.get("email","") or "").strip(),
            "phone": (r.get("phone","") or "").strip(),
            "type": (r.get("type","Customer") or "Customer").strip()
        })
    return rows

def search_registered(query: str, limit: int = 50) -> List[Dict[str, str]]:
    q = (query or "").strip().lower()
    if not q:
        return []
    rows = read_registered_rows()
    out = []
    for r in rows:
        hay = " ".join([
            r.get("customer_acct",""), r.get("first_name",""),
            r.get("last_name",""), r.get("company_name",""),
            r.get("registration_name","")
        ]).lower()
        if q in hay:
            out.append(r)
            if len(out) >= limit:
                break
    return out

def now_iso():
    return datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

def make_walkin_id():
    # WALKIN-YYYYMMDDHHMMSS-XXXX (short random-ish suffix)
    ts = datetime.datetime.utcnow().strftime("%Y%m%d%H%M%S")
    # cheap suffix from time
    suffix = ts[-4:]
    return f"WALKIN-{ts}-{suffix}"

def queue_snapshot() -> List[Dict[str, str]]:
    """
    Read attendance.csv and return items whose latest status is IN_ATTENDANCE.
    """
    cc = _container_client()
    bc = cc.get_blob_client(ATTENDANCE)
    if not bc.exists():
        return []
    text = bc.download_blob().readall().decode("utf-8", errors="ignore")
    f = io.StringIO(text)
    dr = csv.DictReader(f)
    last_by_key = {}
    for r in dr:
        key = f"{r.get('id_type','')}::{r.get('id_value','')}"
        last_by_key[key] = r  # last wins
    out = []
    for r in last_by_key.values():
        if (r.get("status") or "").upper() == "IN_ATTENDANCE":
            out.append(r)
    # sort by time asc so oldest first
    out.sort(key=lambda x: x.get("ts_iso",""))
    return out
