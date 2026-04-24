from fastapi import FastAPI, Query, HTTPException
from container import generate_manifest
import json
import os
from collections import Counter
from datetime import datetime, timezone

# BUAT ENDPOINT utk simulator api manifest BC

app = FastAPI(
    title="Generator API",
    version="1.0.0",
)

@app.get("/")
def root():
    return {"status": "OK", "message": "Generator API is running"}


@app.get("/manifest/import")
def manifest_import(
    num_containers : int = Query(default=200, ge=1, le=1000),
    vessel_name = Query(default="KMA KGM BLUE"),
    voyage = Query(default="0123S"),
    captain_name = Query(default="robert maesse"),
    terminal_code = Query(default="UTC10"),
    call_sign = Query(default="9V7862"),
    imo_number = Query(default="955756"),
    mmsi_number = Query(default="9V09930307862"),
    customs_office_code = Query(default="040300"),
):
    
    result = generate_manifest(
        num_containers=num_containers,
        process_type="01",
        vessel_name=vessel_name,
        voyage=voyage,
        captain_name=captain_name,
        terminal_code=terminal_code,
        call_sign=call_sign,
        imo_number=imo_number,
        mmsi_number=mmsi_number,
        customs_office_code=customs_office_code,
    )

    with open("./batch/data/manifest_import.json", "w") as f:
        json.dump(result, f, indent=2)

    return result


@app.get("/manifest/export")
def manifest_export(
    num_containers : int = Query(default=200, ge=1, le=1000),
    vessel_name = Query(default="MV SINAR JAYA"),
    voyage = Query(default="0456N"),
    captain_name = Query(default="jamly lorava"),
    terminal_code = Query(default="UTC10"),
    call_sign = Query(default="9V7862"),
    imo_number = Query(default="955756"),
    mmsi_number = Query(default="9V09930307862"),
    customs_office_code = Query(default="040300"),
):
    
    result = generate_manifest(
        num_containers=num_containers,
        process_type="02",
        vessel_name=vessel_name,
        voyage=voyage,
        captain_name=captain_name,
        terminal_code=terminal_code,
        call_sign=call_sign,
        imo_number=imo_number,
        mmsi_number=mmsi_number,
        customs_office_code=customs_office_code,
    )

    with open("./batch/data/manifest_export.json", "w") as f:
        json.dump(result, f, indent=2)

    return result

 
MANIFEST_IMPORT_PATH = "./batch/data/manifest_import.json"
MANIFEST_EXPORT_PATH = "./batch/data/manifest_export.json"
 
PROCESS_TYPE_DESC = {"01": "IMPORT", "02": "EXPORT"}
TRANSPORT_MODE_DESC = {"01": "LAUT", "02": "UDARA", "03": "DARAT"}
 
def build_schedule_entry(manifest):
    containers  = manifest.get("containers", [])
    process_type = manifest.get("process_type", "")
    eta_epoch   = manifest.get("eta")
    etd_epoch   = manifest.get("etd")
 
    # Konversi epoch ke ISO string
    def epoch_to_iso(epoch):
        if not epoch:
            return None
        return datetime.fromtimestamp(epoch, tz=timezone.utc).isoformat()
 
    # Summary container
    total = len(containers)
    by_size = dict(Counter(c.get("container_size", "unknown") for c in containers))
    by_type = dict(Counter(c.get("container_type", "unknown") for c in containers))
 
    # Hitung by_status (loaded_flag)
    by_status = dict(Counter(c.get("status_code", "unknown") for c in containers))
 
    return {
        "vessel_id"          : manifest.get("vessel_id"),
        "vessel_name"        : manifest.get("vessel_name"),
        "imo_number"         : manifest.get("imo_number"),
        "call_sign"          : manifest.get("call_sign"),
        "mmsi_number"        : manifest.get("mmsi_number"),
        "voyage_number"      : manifest.get("voyage"),
        "process_type"       : process_type,
        "process_type_desc"  : PROCESS_TYPE_DESC.get(process_type, "UNKNOWN"),
        "transport_mode"     : manifest.get("transport_mode"),
        "transport_mode_desc": TRANSPORT_MODE_DESC.get(manifest.get("transport_mode", ""), "UNKNOWN"),
        "terminal_code"      : manifest.get("terminal_code"),
        "customs_office_code": manifest.get("customs_office_code"),
        "captain_name"       : manifest.get("captain_name"),
        "schedule": {
            "eta": epoch_to_iso(eta_epoch),
            "etd": epoch_to_iso(etd_epoch),
        },
        "containers_summary": {
            "total"  : total,
            "by_size": by_size,
            "by_type": by_type,
            "by_status": by_status,
        },
    }
 
 
@app.get("/vessel/schedule")
def vessel_schedule():
    schedules = []
 
    for path in [MANIFEST_IMPORT_PATH, MANIFEST_EXPORT_PATH]:
        if not os.path.exists(path):
            continue
        with open(path) as f:
            manifest = json.load(f)
        schedules.append(build_schedule_entry(manifest))
 
    return {
        "status"   : "success",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "total"    : len(schedules),
        "data"     : schedules,
    }
 
 
@app.get("/vessel/schedule/{process_type}")
def vessel_schedule_by_type(process_type):
    if process_type not in ("01", "02"):
        raise HTTPException(status_code=400, detail="process_type harus '01' (import) atau '02' (export)")
 
    path = MANIFEST_IMPORT_PATH if process_type == "01" else MANIFEST_EXPORT_PATH
 
    if not os.path.exists(path):
        raise HTTPException(status_code=404, detail=f"Manifest untuk process_type '{process_type}' belum dibuat")
 
    with open(path) as f:
        manifest = json.load(f)
 
    return {
        "status"   : "success",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "data"     : build_schedule_entry(manifest),
    }
 