import json
import random
from datetime import datetime, timezone, timedelta
import paho.mqtt.publish as publish
import time

# bikin Config
MQTT_HOST           = "localhost"
MQTT_PORT           = 1883
MQTT_TOPIC_GATE_IN  = "/gate/in"
MQTT_TOPIC_GATE_OUT = "/gate/out"

YARD_LOG = "../../batch/data/yard_log.json"

DENGAN_ANOMALI = True

GATE_IDS    = ["GATE-01", "GATE-02", "GATE-03", "GATE-04"]
TERMINAL_ID = "TPS-TANJUNG PRIOK"

# buat random nomor truk dan rfid
PLATE_PREFIXES = ["W", "L", "AE", "AG", "AB", "N", "P", "S"]
PLATE_SUFFIXES = ["AB", "CD", "EF", "GH", "IJ", "KL", "MN", "OP"]
DRIVER_IDS     = [f"DRV-{str(i).zfill(4)}" for i in range(1000, 1200)]
RFID_TAGS      = [f"RFID-TPS-{str(i).zfill(5)}" for i in range(10000, 10500)]

TARE_WEIGHT_MIN = 18000
TARE_WEIGHT_MAX = 22000

# counter
seq_in  = 1
seq_out = 1

# bikin helper
def generate_plate_no():
    return f"{random.choice(PLATE_PREFIXES)}-{random.randint(1000,9999)}-{random.choice(PLATE_SUFFIXES)}"

def generate_do_number(date_str, sequence):
    return f"DO-{date_str}-{sequence:04d}"

def generate_ocr(container_no):
    rand = random.random()
    if rand < 0.70:
        return {"container_no": container_no, "confidence_score": round(random.uniform(0.95, 1.00), 2), "status": "SUCCESS"}
    elif rand < 0.90:
        return {"container_no": container_no, "confidence_score": round(random.uniform(0.80, 0.94), 2), "status": "LOW_CONFIDENCE"}
    else:
        return {"container_no": "", "confidence_score": round(random.uniform(0.00, 0.79), 2), "status": "FAILED"}

def generate_rfid():
    return {"rfid_tag": random.choice(RFID_TAGS), "rfid_read_success": random.random() < 0.90}

def generate_weight(load_kg):
    tare = random.randint(TARE_WEIGHT_MIN, TARE_WEIGHT_MAX)
    nett = load_kg + random.randint(-150, 150)
    return {"gross_weight_kg": tare + nett, "tare_weight_kg": tare, "nett_weight_kg": nett}

def generate_anomali(record):
    anomali_type = random.choice(["RFID_FAILED", "OCR_MISMATCH", "WEIGHT_EXTREME", "MISSING_DO"])
    if anomali_type == "RFID_FAILED":
        record["truck"]["rfid_read_success"] = False
        record["truck"]["rfid_tag"]          = ""
    elif anomali_type == "OCR_MISMATCH":
        record["ocr"]["container_no"]     = f"MSCU{random.randint(1000000,9999999)}"
        record["ocr"]["confidence_score"] = round(random.uniform(0.80, 0.94), 2)
        record["ocr"]["status"]           = "LOW_CONFIDENCE"
    elif anomali_type == "WEIGHT_EXTREME":
        record["weight"]["gross_weight_kg"] = random.choice([0, 99999, -100])
    elif anomali_type == "MISSING_DO":
        record["truck"]["do_number"] = ""
    record["anomali_type"] = anomali_type
    return record


# main fungsi utk kirim data ke emqx / mqtt
def publish_gate_out(item, gate_out_ts):
    global seq_out
    container_no  = item["container_number"]
    container_ref = item["container_ref"]
    date_str      = gate_out_ts.strftime("%Y%m%d")
    rfid          = generate_rfid()

    record = {
        "event_id"   : f"GATE-OUT-{date_str}-{seq_out:04d}",
        "timestamp"  : gate_out_ts.isoformat(),
        "gate_id"    : random.choice(GATE_IDS),
        "terminal_id": TERMINAL_ID,
        "direction"  : "OUT",
        "truck": {
            "plate_no"         : generate_plate_no(),
            "rfid_tag"         : rfid["rfid_tag"],
            "rfid_read_success": rfid["rfid_read_success"],
            "driver_id"        : random.choice(DRIVER_IDS),
            "do_number"        : generate_do_number(date_str, seq_out),
        },
        "ocr"   : generate_ocr(container_no),
        "weight": generate_weight(item["load_cell_kg"]),
        "container_ref": {
            "container_id"       : container_ref["container_id"],
            "container_number"   : container_no,
            "container_type"     : container_ref["container_type"],
            "container_type_name": container_ref.get("container_type_name", ""),
            "container_size"     : container_ref["container_size"],
            "container_category" : container_ref["container_category"],
            "seal_number"        : container_ref["seal_number"],
        },
        # "crane_event_ref": {
        #     "crane_event_id": item["crane_event_id"],
        #     "crane_id"      : item["crane_id"],
        #     "operation"     : "DISCHARGE",
        # },
        "status"      : "RELEASED",
        "anomali_type": None,
    }

    if DENGAN_ANOMALI and random.random() < 0.05:
        record = generate_anomali(record)

    ts_str = gate_out_ts.strftime("%Y-%m-%d %H:%M:%S")
    publish.single(MQTT_TOPIC_GATE_OUT, payload=json.dumps(record), hostname=MQTT_HOST, port=MQTT_PORT)
    print(f"  [GATE-OUT-{seq_out:04d}] {record['gate_id']} | {ts_str} | "
          f"{record['truck']['plate_no']} | {container_no} | {record['anomali_type'] or 'NORMAL'}")
    seq_out += 1

# main fungsi , utk kirim data ke emqx / mqtt , kita buat  1-4 jam sudah di load di kapal
def publish_gate_in(item: dict, gate_in_ts: datetime):
    global seq_in
    container_no  = item["container_number"]
    container_ref = item["container_ref"]
    date_str      = gate_in_ts.strftime("%Y%m%d")
    rfid          = generate_rfid()

    record = {
        "event_id"   : f"GATE-IN-{date_str}-{seq_in:04d}",
        "timestamp"  : gate_in_ts.isoformat(),
        "gate_id"    : random.choice(GATE_IDS),
        "terminal_id": TERMINAL_ID,
        "direction"  : "IN",
        "truck": {
            "plate_no"         : generate_plate_no(),
            "rfid_tag"         : rfid["rfid_tag"],
            "rfid_read_success": rfid["rfid_read_success"],
            "driver_id"        : random.choice(DRIVER_IDS),
            "do_number"        : generate_do_number(date_str, seq_in),
        },
        "ocr"   : generate_ocr(container_no),
        "weight": generate_weight(item["load_cell_kg"]),
        "container_ref": {
            "container_id"       : container_ref["container_id"],
            "container_number"   : container_no,
            "container_type"     : container_ref["container_type"],
            "container_type_name": container_ref.get("container_type_name", ""),
            "container_size"     : container_ref["container_size"],
            "container_category" : container_ref["container_category"],
            "seal_number"        : container_ref["seal_number"],
        },
        # "crane_event_ref": {
        #     "crane_event_id": item["crane_event_id"],
        #     "crane_id"      : item["crane_id"],
        #     "operation"     : "LOAD",
        # },
        "status"      : "APPROVED",
        "anomali_type": None,
    }

    if DENGAN_ANOMALI and random.random() < 0.05:
        record = generate_anomali(record)

    ts_str = gate_in_ts.strftime("%Y-%m-%d %H:%M:%S")
    publish.single(MQTT_TOPIC_GATE_IN, payload=json.dumps(record), hostname=MQTT_HOST, port=MQTT_PORT)
    print(f"  [GATE-IN-{seq_in:04d}]  ✅ {record['gate_id']} | {ts_str} | "
          f"{record['truck']['plate_no']} | {container_no} | "
          f"{record['anomali_type'] or 'NORMAL'}")
    seq_in += 1

if __name__ == "__main__":

    # ambil data dari yardlog.json
    with open(YARD_LOG) as f:
        yard_log = json.load(f)

    random.shuffle(yard_log)

    total_discharge = sum(1 for i in yard_log if i["operation"] == "DISCHARGE")
    total_load      = sum(1 for i in yard_log if i["operation"] == "LOAD")

    try:
        for item in yard_log:
            operation = item["operation"]

            if operation == "DISCHARGE":
                gate_out_ts = datetime.fromisoformat(item["crane_timestamp"].replace("Z", "+00:00")) + timedelta(
                    hours=random.choices(
                        [random.uniform(4, 6), random.uniform(6, 12), random.uniform(12, 26), random.uniform(26, 42), random.uniform(48, 72)],
                        weights=[5, 5, 40, 30, 20]
                    )[0]
                )
                publish_gate_out(item, gate_out_ts)

            elif operation == "LOAD":
                gate_in_ts = datetime.fromisoformat(item["gate_in_timestamp"].replace("Z", "+00:00"))
                publish_gate_in(item, gate_in_ts)

            time.sleep(1)

        print(f"Selesai. GATE IN: {seq_in-1} | GATE OUT: {seq_out-1}")

    except KeyboardInterrupt:
        print(f"Stopped. GATE IN: {seq_in-1} | GATE OUT: {seq_out-1}")

