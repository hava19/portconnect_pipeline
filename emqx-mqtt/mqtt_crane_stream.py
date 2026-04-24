import json
import random
import time
from datetime import datetime, timezone, timedelta
import paho.mqtt.publish as publish

# bikin Config 
MQTT_HOST    = "localhost"
MQTT_PORT    = 1883
MQTT_TOPIC   = "/crane/events"
INTERVAL_DETIK = 1
DENGAN_ANOMALI = True

MANIFEST_IMPORT = "../../batch/data/manifest_import.json"
MANIFEST_EXPORT = "../../batch/data/manifest_export.json"
TERMINAL_ID = "TPS-TANJUNG PRIOK"

# Random GPS
GPS_BASE = {
    "CRANE-01": {"lat": -7.194589, "lng": 112.891456},
    "CRANE-02": {"lat": -7.194712, "lng": 112.891623},
    "CRANE-03": {"lat": -7.194834, "lng": 112.891789},
    "CRANE-04": {"lat": -7.194956, "lng": 112.891934},
    "CRANE-05": {"lat": -7.195078, "lng": 112.892100},
    "CRANE-06": {"lat": -7.195200, "lng": 112.892267},
    "CRANE-07": {"lat": -7.195322, "lng": 112.892433},
    "CRANE-08": {"lat": -7.195444, "lng": 112.892600},
}

# pisah crane bongkar dan muat , menghindari duplikat dalam 1 waktu
CRANE_DISCHARGE = ["CRANE-01", "CRANE-02", "CRANE-03", "CRANE-04"]  # Import: turunkan dari kapal
CRANE_LOAD      = ["CRANE-05", "CRANE-06", "CRANE-07", "CRANE-08"]  # Export: naikkan ke kapal

_rr_discharge = 0
_rr_load      = 0

# Jenis container nanti ditransform spark
# definisi berat container
CONTAINER_WEIGHT = {
    "1": {"min": 5000,  "max": 30480},   # Dry
    "2": {"min": 5000,  "max": 30480},   # Tunne Type
    "3": {"min": 5000,  "max": 28000},   # Open Top
    "4": {"min": 4000,  "max": 25000},   # Flat Rack
    "5": {"min": 8000,  "max": 34000},   # Reefer
    "6": {"min": 5000,  "max": 30000},   # Barge
    "7": {"min": 5000,  "max": 30000},   # Bulk
    "8": {"min": 10000, "max": 26000},   # ISO Tank
}

# load data dari manifest export dan import , 
# supaya kontainer tidak ada duplikat di simulator
def load_containers(filepath: str):
    with open(filepath) as f:
        manifest = json.load(f)
    return manifest["containers"]

# generate random ocr 
def generate_ocr(container_no: str):
    status = random.choices(
        ["SUCCESS", "LOW_CONFIDENCE", "FAILED"],
        weights=[86, 9, 5], # ini persentase nya sucess 86 % dst
        k=1
    )[0] #ambil yang pertam

    if status == "SUCCESS":
        return {"container_no": container_no, "confidence_score": round(random.uniform(0.95, 1.00), 2), "status": "SUCCESS"}
    elif status == "LOW_CONFIDENCE":
        return {"container_no": container_no, "confidence_score": round(random.uniform(0.80, 0.94), 2), "status": "LOW_CONFIDENCE"}
    else:
        return {"container_no": "", "confidence_score": round(random.uniform(0.00, 0.79), 2), "status": "FAILED"}


# generate event proses bagaimana crane menghasilkan data 
# disini kunci nya , data yang dihasilkan akan di kirim ke mqtt lalu ke kafka
def generate_event(sequence: int, container: dict, operation: str) -> dict:
    global _rr_discharge, _rr_load

    if operation == "DISCHARGE":
        crane_id      = CRANE_DISCHARGE[_rr_discharge % len(CRANE_DISCHARGE)]
        _rr_discharge += 1
    else:
        crane_id   = CRANE_LOAD[_rr_load % len(CRANE_LOAD)]
        _rr_load  += 1

    gps_base   = GPS_BASE[crane_id]

    # Majukan jam crane ini 10-15 menit dari event terakhirnya
    crane_clock[crane_id] += timedelta(minutes=random.randint(10, 15))
    ctype     = container.get("container_type", "1")
    weight_cfg = CONTAINER_WEIGHT.get(ctype, {"min": 5000, "max": 30000})

    event = {
        "event_id"          : f"CRN-{datetime.now().strftime('%Y%m%d')}-{sequence:06d}",
        "timestamp"         : crane_clock[crane_id].isoformat(),
        "terminal_id"       : TERMINAL_ID,
        "crane_id"          : crane_id,
        "operation"         : operation,
        "spreader_height_m" : round(random.uniform(8.0, 40.0), 1),
        "trolley_position_m": round(random.uniform(0.0, 35.0), 1),
        "hoist_speed_mps"   : round(random.uniform(0.5, 1.2), 2),
        "cycle_time_sec"    : random.randint(60, 120),
        "load_cell_kg"      : random.randint(weight_cfg["min"], weight_cfg["max"]),
        "spreader_locked"   : random.random() < 0.80,
        "gps_lat"           : round(gps_base["lat"] + random.uniform(-0.0005, 0.0005), 6),
        "gps_lng"           : round(gps_base["lng"] + random.uniform(-0.0005, 0.0005), 6),
        "ocr"               : generate_ocr(container["container_number"]),
        "container_ref": {
            "container_id"      : container["container_id"],
            "container_number"  : container["container_number"],
            "container_type"    : container["container_type"],
            "container_type_name": container.get("container_type_name", ""),
            "container_size"    : container["container_size"],
            "container_category": container["container_category"],
            "seal_number"       : container["seal_number"],
        },
        "anomali_type": None,
    }

    # Sisipkan anomali ~5%
    if DENGAN_ANOMALI and random.random() < 0.05:
        event["anomali_type"] = random.choice(["WEIGHT_EXTREME", "SPEED_EXTREME", "MISSING_OCR"])
        if event["anomali_type"] == "WEIGHT_EXTREME":
            event["load_cell_kg"] = random.choice([0, 99999, -100])
        elif event["anomali_type"] == "SPEED_EXTREME":
            event["hoist_speed_mps"] = round(random.uniform(2.5, 5.0), 2)
        elif event["anomali_type"] == "MISSING_OCR":
            event["ocr"] = {"container_no": "", "confidence_score": 0.0, "status": "FAILED"}

    return event


if __name__ == "__main__":
    # Load kontainer per manifest
    import_containers = load_containers(MANIFEST_IMPORT)
    export_containers = load_containers(MANIFEST_EXPORT)

    # Shuffle agar urutan random
    random.shuffle(import_containers)
    random.shuffle(export_containers)

    # Gabung: import → DISCHARGE, export → LOAD
    # dan tandai
    queue = (
        [{"container": c, "operation": "DISCHARGE"} for c in import_containers] +
        [{"container": c, "operation": "LOAD"}      for c in export_containers]
    )
    random.shuffle(queue)  # campur DISCHARGE & LOAD

    total   = len(queue)

    # Jam internal per crane, mulai dari waktu sekarang
    now = datetime.now(timezone.utc)
    crane_clock = {crane_id: now for crane_id in list(CRANE_DISCHARGE) + list(CRANE_LOAD)}

    sequence = 1
    try:
        # Setelah queue dibentuk, jangan langsung stream
        # Build dulu semua event dengan timestamp-nya
        events_with_time = []
        for item in queue:
            container = item["container"]
            operation = item["operation"]
            event     = generate_event(sequence, container, operation)
            events_with_time.append(event)
            sequence += 1

        # Simpan yard log untuk patokan gate simulator
        yard_log = []
        for event in events_with_time:
            container_no = event["ocr"]["container_no"]
            if not container_no:
                continue  # skip OCR FAILED

            if event["operation"] == "DISCHARGE":
                yard_log.append({
                    "container_number" : event["container_ref"]["container_number"],
                    "container_id"     : event["container_ref"]["container_id"],
                    "operation"        : "DISCHARGE",
                    "crane_event_id"   : event["event_id"],
                    "crane_id"         : event["crane_id"],
                    "crane_timestamp"  : event["timestamp"],
                    "load_cell_kg"     : event["load_cell_kg"],
                    "container_ref"    : event["container_ref"],
                })

            elif event["operation"] == "LOAD":
                crane_ts   = datetime.fromisoformat(event["timestamp"].replace("Z", "+00:00"))
                gate_in_ts = crane_ts - timedelta(hours=random.uniform(1, 4))
                yard_log.append({
                    "container_number"  : event["container_ref"]["container_number"],
                    "container_id"      : event["container_ref"]["container_id"],
                    "operation"         : "LOAD",
                    "crane_event_id"    : event["event_id"],
                    "crane_id"          : event["crane_id"],
                    "crane_timestamp"   : event["timestamp"],
                    "gate_in_timestamp" : gate_in_ts.isoformat(),
                    "load_cell_kg"      : event["load_cell_kg"],
                    "container_ref"     : event["container_ref"],
                })

        with open("../../batch/data/yard_log.json", "w") as f:
            json.dump(yard_log, f, indent=2)
        print(f"   yard_log.json disimpan: {len(yard_log)} container\n")

        # Sort berdasarkan timestamp agar kronologis
        events_with_time.sort(key=lambda e: e["timestamp"])

        # Baru stream
        for i, event in enumerate(events_with_time, start=1):
            publish.single(
                MQTT_TOPIC,
                payload=json.dumps(event),
                hostname=MQTT_HOST,
                port=MQTT_PORT,
            )
            ts_str = datetime.fromisoformat(
                event["timestamp"].replace("Z", "+00:00")
            ).strftime("%Y-%m-%d %H:%M:%S")
            print(
                f"[{i}/{total}] {event['crane_id']} | {ts_str} | "
                f"{event['operation']} | {event['container_ref']['container_number']} | "
                f"{event.get('anomali_type') or 'NORMAL'}"
            )
            time.sleep(INTERVAL_DETIK)

        print(f"Semua {total} container selesai di-stream.")

    except KeyboardInterrupt:
        print(f"Stopped. Total {sequence-1} events sent.")

    