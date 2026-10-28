"""Generate synthetic aviation NDT landing files for the Databricks capstone.

Run from the folder where you want ./ndt_landing to appear:
    python generate_ndt_landing.py
Fixed seed, so the output is identical every run.
"""
import csv
import datetime as dt
import json
import random
import shutil
import string
from pathlib import Path

random.seed(42)
OUT = Path("ndt_landing")
shutil.rmtree(OUT, ignore_errors=True)
OUT.mkdir(parents=True)
D = dt.date
NULL = "\\N"  # CSV missing-value marker used in some files


# ---------------------------------------------------------------- helpers
def write_csv(path, header, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(rows)


def write_jsonl(path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")


def write_json_pretty(path, obj):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2, ensure_ascii=False)
        f.write("\n")


def rand_day(start, end):
    while True:
        d = start + dt.timedelta(days=random.randint(0, (end - start).days))
        if d.weekday() < 5 or random.random() < 0.05:
            return d


# ---------------------------------------------------------------- facilities
FACILITIES = [
    ["FAC-001", "Base Maintenance Hangar 1", "Base maintenance", "APPR-NDT-001"],
    ["FAC-002", "NDT Central Workshop", "NDT workshop", "APPR-NDT-002"],
    ["FAC-003", "Engine Shop", "Engine shop", "APPR-NDT-003"],
    ["FAC-004", "Line Station North", "Line maintenance", NULL],
    ["FAC-005", "Landing Gear Shop", "Component shop", "APPR-NDT-005"],
    ["FAC-006", "Composite Repair Shop", "Component shop", "APPR-NDT-006"],
]
write_csv(OUT / "facilities.csv",
          ["facility_id", "facility_name", "facility_type", "ndt_approval_ref"], FACILITIES)

# ---------------------------------------------------------------- methods
METHODS = [
    {"method_id": "UT", "method_name": "Ultrasonic Testing", "technique_family": "volumetric"},
    {"method_id": "ET", "method_name": "Eddy Current Testing", "technique_family": "surface"},
    {"method_id": "PT", "method_name": "Liquid Penetrant Testing", "technique_family": "surface"},
    {"method_id": "MT", "method_name": "Magnetic Particle Testing", "technique_family": "surface"},
    {"method_id": "RT", "method_name": "Radiographic Testing", "technique_family": "volumetric"},
    {"method_id": "VT", "method_name": "Visual Testing", "technique_family": "surface"},
]
write_jsonl(OUT / "methods.json", METHODS)

MAT_METHODS = {
    "Aluminium 7075-T6": ["UT", "UT", "ET", "ET", "PT", "VT"],
    "Aluminium 2024-T3": ["ET", "ET", "UT", "PT", "VT"],
    "Titanium Ti-6Al-4V": ["PT", "PT", "ET", "UT", "RT", "VT"],
    "Steel 4340": ["MT", "MT", "MT", "UT", "ET", "VT", "PT"],
    "Inconel 718": ["PT", "PT", "ET", "RT", "VT"],
    "CFRP composite": ["UT", "UT", "UT", "VT"],
}
INDICATION_TYPES = {
    "UT": ["corrosion", "delamination", "void"],
    "ET": ["crack"],
    "PT": ["crack", "porosity"],
    "MT": ["crack"],
    "RT": ["porosity", "inclusion", "crack"],
    "VT": ["corrosion", "dent", "scratch"],
}
LOCATIONS = ["fastener hole row", "web-to-flange radius", "trailing edge", "leading edge",
             "lug bore", "attachment flange", "root fillet", "mid-span", "forward face", "aft face"]

# ---------------------------------------------------------------- components
# (description, material, ata chapter, facility numbers, nominal thickness mm)
TYPES = [
    ("Wing rear spar", "Aluminium 7075-T6", "57", [1, 2], 8.0),
    ("Wing front spar", "Aluminium 7075-T6", "57", [1, 2], 8.0),
    ("Fuselage frame", "Aluminium 2024-T3", "53", [1, 2], 2.5),
    ("Fuselage skin panel", "Aluminium 2024-T3", "53", [1, 4], 1.6),
    ("Elevator skin", "CFRP composite", "55", [6, 1], 3.2),
    ("Rudder honeycomb panel", "CFRP composite", "55", [6], 3.2),
    ("Landing gear drag strut", "Steel 4340", "32", [5], 6.0),
    ("Main landing gear axle", "Steel 4340", "32", [5], 12.0),
    ("Nose gear torque link", "Steel 4340", "32", [5, 4], 6.0),
    ("Flap track", "Steel 4340", "27", [1, 2], 6.0),
    ("Engine fan blade", "Titanium Ti-6Al-4V", "72", [3], 4.0),
    ("Engine compressor disk", "Titanium Ti-6Al-4V", "72", [3], 10.0),
    ("Turbine blade", "Inconel 718", "72", [3], 3.0),
    ("Pylon attach fitting", "Titanium Ti-6Al-4V", "54", [1, 2], 9.0),
]
components = []
used_sn = set()
for i in range(120):
    ti = i % len(TYPES)
    t = TYPES[ti]
    while True:
        sn = f"SN{random.randint(100000, 999999)}"
        if sn not in used_sn:
            used_sn.add(sn)
            break
    components.append({
        "id": f"COMP-{i + 1:04d}", "desc": t[0], "pn": f"PN-{t[2]}-{1000 + ti * 37}",
        "sn": sn, "mat": t[1], "ata": t[2],
        "fac": [f"FAC-{n:03d}" for n in t[3]], "nominal": t[4],
    })
missing_sn = random.sample(range(len(components)), 3)
rows = []
for i, c in enumerate(components):
    rows.append([c["id"], c["desc"], c["pn"], NULL if i in missing_sn else c["sn"], c["mat"], c["ata"]])
write_csv(OUT / "components.csv",
          ["component_id", "component_description", "component_partnumber",
           "component_serialnumber", "component_material_type", "component_ata_chapter"], rows)
comp_by_id = {c["id"]: c for c in components}

# ---------------------------------------------------------------- work orders
WO_TYPES = ["Scheduled check"] * 11 + ["Repair"] * 4 + ["Unscheduled"] * 3 + ["Overhaul"] * 2
atas = sorted({c["ata"] for c in components})
wos = []
counters = {}
for d in sorted(rand_day(D(2023, 12, 20), D(2025, 1, 26)) for _ in range(185)):
    counters[d.year] = counters.get(d.year, 0) + 1
    reg = "CN-" + "".join(random.choices(string.ascii_uppercase, k=3))
    wos.append({
        "id": f"WO-{d.year}-{counters[d.year]:04d}", "opened": d,
        "tc": f"TC-{random.choice(atas)}-{random.randint(10, 60)}-{random.randint(100, 399)}",
        "type": random.choice(WO_TYPES), "reg": NULL if random.random() < 0.05 else reg,
    })
write_csv(OUT / "workorders.csv",
          ["workorder_id", "task_card_reference", "workorder_type", "aircraft_reg", "opened_date"],
          [[w["id"], w["tc"], w["type"], w["reg"], w["opened"].strftime("%d/%m/%Y")] for w in wos])


def pick_wo(d):
    c = [w for w in wos if w["opened"] <= d <= w["opened"] + dt.timedelta(days=21)]
    return random.choice(c)["id"]


# ---------------------------------------------------------------- equipment + calibration
EQUIP = [
    ("EQ-001", "Olympus EPOCH 650", "UT", "EP650-C", D(2023, 2, 10), 365),
    ("EQ-002", "Olympus EPOCH 650", "UT", "EP650-C", D(2023, 5, 22), 365),
    ("EQ-003", "Waygate USM Go", "UT", "USMGO-A", D(2023, 9, 4), 365),
    ("EQ-004", "Zetec MIZ-21C", "ET", "MIZ21C-B", D(2023, 6, 15), 365),
    ("EQ-005", "Zetec MIZ-21C", "ET", "MIZ21C-B", D(2023, 3, 28), 365),
    ("EQ-006", "Magnaflux Y-7 Yoke", "MT", "Y7-AC", D(2023, 1, 16), 183),
    ("EQ-007", "Magnaflux Y-7 Yoke", "MT", "Y7-AC", D(2023, 4, 3), 183),
    ("EQ-008", "Portable X-ray source 200kV", "RT", "XR200", D(2023, 7, 19), 365),
    ("EQ-009", "Portable X-ray source 300kV", "RT", "XR300", D(2023, 11, 6), 365),
]
GAP_EQ, GAP_DAYS = "EQ-004", 45  # planted: calibration lapse
cycles = {}
for eid, desc, m, pn, start, interval in EQUIP:
    cs, k = [], 0
    while True:
        on = start + dt.timedelta(days=k * interval + (GAP_DAYS if (eid == GAP_EQ and k >= 1) else 0))
        if on > D(2025, 6, 30):
            break
        due = on + dt.timedelta(days=interval - 1)
        cs.append({"certificate_no": f"CAL-{eid}-{k + 1:02d}", "equipment_id": eid,
                   "calibrated_on": on, "calibration_due": due,
                   "calibration_lab": random.choice(["Accredited Cal Lab A", "Accredited Cal Lab B"]),
                   "result": "PASS"})
        k += 1
    cycles[eid] = cs
    write_json_pretty(OUT / "calibration-records" / f"calibration_{eid}.json",
                      [{**c, "calibrated_on": c["calibrated_on"].isoformat(),
                        "calibration_due": c["calibration_due"].isoformat()} for c in cs])
GAP_START = cycles[GAP_EQ][0]["calibration_due"]
GAP_END = cycles[GAP_EQ][1]["calibrated_on"]


def cal_valid(eid, d):
    return any(c["calibrated_on"] <= d <= c["calibration_due"] for c in cycles[eid])


equip_rows = []
for eid, desc, m, pn, start, interval in EQUIP:
    cur = [c for c in cycles[eid] if c["calibrated_on"] <= D(2025, 1, 31)][-1]
    equip_rows.append({"equipment_id": eid, "description": desc, "partnumber": pn,
                       "serialnumber": f"SN-{eid[-3:]}-{random.randint(1000, 9999)}",
                       "method_id": m, "calibrated_on": cur["calibrated_on"].isoformat(),
                       "calibration_due": cur["calibration_due"].isoformat()})
write_jsonl(OUT / "equipment.json", equip_rows)

# ---------------------------------------------------------------- inspectors
# (id, forename, surname, home facility, [(auth_no, level, valid_from, valid_to, expiry)])
INSPECTORS = [
    ("INS-001", "James", "Whitfield", "FAC-002", [("NDT-AUTH-1001", 3, D(2021, 3, 1), None, D(2026, 2, 28))]),
    ("INS-002", "Rashid", "Al-Farsi", "FAC-002",
     [("NDT-AUTH-1002", 2, D(2022, 2, 1), D(2024, 5, 31), D(2026, 1, 31)),
      ("NDT-AUTH-1012", 3, D(2024, 6, 1), None, D(2027, 5, 31))]),
    ("INS-003", "Sofia", "Marchetti", "FAC-001", [("NDT-AUTH-1003", 2, D(2022, 9, 1), None, D(2026, 8, 31))]),
    ("INS-004", "Amina", "Benali", "FAC-005", [("NDT-AUTH-1004", 2, D(2021, 11, 1), None, D(2026, 10, 31))]),
    ("INS-005", "Tomasz", "Kowalski", "FAC-003", [("NDT-AUTH-1005", 3, D(2020, 5, 1), None, D(2026, 4, 30))]),
    ("INS-006", "Priya", "Raman", "FAC-001", [("NDT-AUTH-1006", 2, D(2023, 1, 1), None, D(2027, 12, 31))]),
    ("INS-007", "Luca", "Ferrero", "FAC-004",
     [("NDT-AUTH-1007", 2, D(2023, 6, 1), D(2024, 9, 30), D(2026, 5, 31)),
      ("NDT-AUTH-1017", 3, D(2024, 10, 1), None, D(2027, 9, 30))]),
    ("INS-008", "Karim", "Haddad", "FAC-006", [("NDT-AUTH-1008", 2, D(2022, 4, 1), None, D(2026, 3, 31))]),
    ("INS-009", "Elena", "Petrova", "FAC-002", [("NDT-AUTH-1009", 2, D(2024, 5, 1), None, D(2027, 4, 30))]),
    ("INS-010", "Daniel", "Okafor", "FAC-003", [("NDT-AUTH-1010", 3, D(2019, 9, 1), None, D(2026, 8, 31))]),
]
ins_rows, hist_rows = [], []
for iid, fn, sn, fac, hist in INSPECTORS:
    cur = hist[-1]
    ins_rows.append({"inspector_id": iid, "name": {"forename": fn, "surname": sn},
                     "home_facility_id": fac, "employment_status": "active",
                     "authorization": {"number": cur[0], "max_en4179_level": cur[1],
                                       "expiry": cur[4].isoformat()}})
    for a, lvl, vf, vt, ex in hist:
        hist_rows.append([iid, a, lvl, ex.isoformat(), vf.isoformat(), vt.isoformat() if vt else NULL])
write_jsonl(OUT / "inspectors.json", ins_rows)
write_csv(OUT / "certification_history.csv",
          ["inspector_id", "authorization_number", "max_en4179_level", "authorization_expiry",
           "valid_from", "valid_to"], hist_rows)


def active_inspectors(d):
    return [i[0] for i in INSPECTORS
            if any(vf <= d and (vt is None or d <= vt) for _, _, vf, vt, _ in i[4])]


# ---------------------------------------------------------------- inspection generator
DUR = {"UT": (60, 150), "ET": (30, 90), "PT": (40, 80), "MT": (30, 70), "RT": (90, 180), "VT": (15, 45)}


def pick_equipment(method, d):
    cands = [e[0] for e in EQUIP if e[2] == method]
    if not cands:
        return None
    if method == "ET" and GAP_START < d < GAP_END and random.random() < 0.35:
        return GAP_EQ
    valid = [e for e in cands if cal_valid(e, d)]
    return random.choice(valid or cands)


def gen_inspection(d):
    comp = random.choice(components)
    method = random.choice(MAT_METHODS[comp["mat"]])
    fac = random.choice(comp["fac"])
    if method == "RT" and fac not in ("FAC-002", "FAC-003"):
        fac = "FAC-002"
    fail = random.random() < 0.06
    n_ind = random.randint(1, 4) if fail else (1 if random.random() < 0.08 else 0)
    inds = []
    for k in range(n_ind):
        length = round(random.uniform(3, 18), 1) if fail else round(random.uniform(0.5, 2.0), 1)
        width = round(random.uniform(0.1, 1.5), 1) if fail else round(random.uniform(0.1, 0.5), 1)
        inds.append({"indication_no": k + 1, "location": random.choice(LOCATIONS),
                     "type": random.choice(
                         (["delamination", "void"] if comp["mat"] == "CFRP composite" else ["corrosion", "void"])
                         if method == "UT" else INDICATION_TYPES[method]),
                     "length_mm": length, "width_mm": width,
                     "disposition": random.choice(["accept", "monitor"])})
    if fail:
        max(inds, key=lambda x: x["length_mm"])["disposition"] = "reject"
    lo, hi = DUR[method]
    dur = random.randint(lo, hi)
    if fail:
        dur = int(dur * 1.2)
    nxt = random.choice([250, 500, 1000]) if fail else random.choice([2000, 3000, 4000, 6000])
    rec = {
        "inspection_id": None, "inspection_date": d.isoformat(), "component_id": comp["id"],
        "workorder_id": pick_wo(d), "method_id": method, "equipment_id": pick_equipment(method, d),
        "facility_id": fac, "inspector_id": random.choice(active_inspectors(d)),
        "duration_min": dur, "result": "FAIL" if fail else "PASS", "indication_count": n_ind,
        "max_indication_length_mm": max((i["length_mm"] for i in inds), default=0.0),
        "max_indication_width_mm": max((i["width_mm"] for i in inds), default=0.0),
        "next_inspection_due_hrs": nxt,
    }
    readings = []
    if method == "UT":
        npts = random.randint(6, 10)
        loss_pts = set(random.sample(range(npts), random.randint(1, 3))) if (fail and random.random() < 0.7) else set()
        for p in range(npts):
            loss = random.uniform(0.10, 0.25) if p in loss_pts else abs(random.gauss(0.01, 0.01))
            t = None if random.random() < 0.02 else round(comp["nominal"] * (1 - loss), 2)
            readings.append({"point_ref": f"P{p + 1:02d}", "thickness_mm": t,
                             "nominal_mm": comp["nominal"], "probe_id": f"PRB-{random.randint(1, 5):02d}"})
    return {"rec": rec, "inds": inds, "readings": readings, "date": d}


def assign_ids(items, year):
    items.sort(key=lambda x: x["date"])
    for n, it in enumerate(items, 1):
        it["rec"]["inspection_id"] = f"INSP-{year}-{n:04d}"


# ---------------------------------------------------------------- MAIN LOAD (2024)
main = [gen_inspection(rand_day(D(2024, 1, 2), D(2024, 12, 31))) for _ in range(1200)]
assign_ids(main, 2024)
base_by_id = {it["rec"]["inspection_id"]: dict(it["rec"]) for it in main}

# findings (multi-line pretty JSON array)
write_json_pretty(OUT / "findings.json", [
    {"finding_id": f"FND-{n:05d}", "inspection_id": it["rec"]["inspection_id"], "indications": it["inds"]}
    for n, it in enumerate([i for i in main if i["inds"]], 1)])

# UT thickness readings, one CSV per inspection date
by_date, rid = {}, 0
for it in main:
    for r in it["readings"]:
        rid += 1
        by_date.setdefault(it["date"], []).append(
            [f"RD-{rid:07d}", it["rec"]["inspection_id"], r["point_ref"],
             NULL if r["thickness_mm"] is None else r["thickness_mm"], r["nominal_mm"], r["probe_id"]])
for d, rws in by_date.items():
    write_csv(OUT / "ut-thickness-readings" / f"readings_{d.isoformat()}.csv",
              ["reading_id", "inspection_id", "point_ref", "thickness_mm", "nominal_mm", "probe_id"], rws)

# ---- planted data problems (documented in ANSWER_KEY file)
out = [dict(it["rec"]) for it in main]
pool = list(range(len(out)))
random.shuffle(pool)
key = {}


def take(k, cond=lambda r: True):
    chosen = []
    for i in list(pool):
        if cond(out[i]):
            chosen.append(i)
            pool.remove(i)
            if len(chosen) == k:
                break
    return chosen


idx = take(3)
for i in idx:
    out[i]["duration_min"] = -out[i]["duration_min"]
key["negative_duration"] = [out[i]["inspection_id"] for i in idx]
idx = take(3, lambda r: r["indication_count"] == 0 and r["method_id"] != "UT")
for n, i in enumerate(idx):
    out[i]["component_id"] = f"COMP-9{n + 1:02d}"
key["orphan_component"] = [out[i]["inspection_id"] for i in idx]
idx = take(6, lambda r: r["indication_count"] == 0)
for i in idx:
    out[i]["max_indication_length_mm"] = None
    out[i]["max_indication_width_mm"] = None
key["null_measurements"] = [out[i]["inspection_id"] for i in idx]
idx = take(8, lambda r: r["equipment_id"] is not None)
for i in idx:
    out[i]["equipment_id"] = ""
key["empty_string_equipment"] = [out[i]["inspection_id"] for i in idx]
idx = take(7)
for i in idx:
    out[i]["facility_id"] += " "
key["trailing_space_facility"] = [out[i]["inspection_id"] for i in idx]
idx = take(5)
for i in idx:
    y, m, dd = out[i]["inspection_date"].split("-")
    out[i]["inspection_date"] = f"{dd}/{m}/{y}"
key["ddmmyyyy_date"] = [out[i]["inspection_id"] for i in idx]
for r in random.sample(out, int(0.12 * len(out))):
    r["result"] = random.choice([r["result"].lower(), r["result"].title()])
dups = random.sample(out, 10)
key["duplicate_lines"] = sorted({d["inspection_id"] for d in dups})
for d in dups:
    out.insert(random.randint(0, len(out)), dict(d))
write_jsonl(OUT / "inspections.json", out)
lapsed = [it["rec"]["inspection_id"] for it in main
          if it["rec"]["equipment_id"] and not cal_valid(it["rec"]["equipment_id"], it["date"])]
key["calibration_lapse"] = lapsed

# ---------------------------------------------------------------- INCREMENTAL (Jan 2025)
seq = 0
batches = [(D(2025, 1, 6), 24), (D(2025, 1, 13), 28), (D(2025, 1, 20), 26)]
prev_new = None
for bi, (wk, n) in enumerate(batches):
    items = []
    for _ in range(n):
        d = wk + dt.timedelta(days=random.randint(0, 4))
        items.append(gen_inspection(d))
    items.sort(key=lambda x: x["date"])
    for it in items:
        seq += 1
        it["rec"]["inspection_id"] = f"INSP-2025-{seq:04d}"
    recs = [it["rec"] for it in items]
    if bi == 1:  # corrections of the negative durations from the main load
        for iid in key["negative_duration"][:2]:
            recs.append(base_by_id[iid])
        key["corrected_in_batch"] = key["negative_duration"][:2]
    if bi == 2:  # exact resend of a record already delivered in batch 2
        recs.append(dict(prev_new))
        key["resent_in_batch3"] = prev_new["inspection_id"]
    if bi == 1:
        prev_new = dict(recs[0])
    folder = OUT / "incremental" / wk.isoformat()
    write_jsonl(folder / "inspections.json", recs)
    write_json_pretty(folder / "findings.json", [
        {"finding_id": f"FND-{wk.strftime('%m%d')}-{k:03d}", "inspection_id": it["rec"]["inspection_id"],
         "indications": it["inds"]} for k, it in enumerate([i for i in items if i["inds"]], 1)])

# ---------------------------------------------------------------- documentation
n_files = sum(1 for p in OUT.rglob("*") if p.is_file())
(OUT / "README.md").write_text(f"""# NDT landing files

Synthetic aviation NDT data, generated by `generate_ndt_landing.py` (seed 42). Mirrors the
star schema in the ndt-data-model repo. Upload the contents of this folder to
`/Volumes/ndt/landing/files/` keeping the folder structure.

| Path | Format | Rows | Notes |
| --- | --- | --- | --- |
| facilities.csv | CSV, header | {len(FACILITIES)} | `\\N` marks a missing value |
| components.csv | CSV, header | {len(components)} | `\\N` marks a missing value |
| workorders.csv | CSV, header | {len(wos)} | `opened_date` is dd/MM/yyyy |
| methods.json | JSON lines | {len(METHODS)} | one object per line |
| equipment.json | JSON lines | {len(EQUIP)} | current calibration only |
| inspectors.json | JSON lines | {len(INSPECTORS)} | nested `name` and `authorization` structs |
| certification_history.csv | CSV, header | {len(hist_rows)} | authorization history per inspector |
| inspections.json | JSON lines | {len(out)} | main fact, calendar year 2024 |
| findings.json | multi-line JSON array | {sum(1 for i in main if i['inds'])} | nested `indications` array |
| ut-thickness-readings/ | CSV, one file per day | {len(by_date)} files | UT inspections only |
| calibration-records/ | multi-line JSON, one file per instrument | {len(EQUIP)} files | calibration history |
| incremental/<file_date>/ | inspections.json + findings.json | 3 weekly batches | January 2025 |

Total files: {n_files}.

## Columns

**inspections.json**: inspection_id, inspection_date, component_id, workorder_id, method_id,
equipment_id (null for PT and VT), facility_id, inspector_id, duration_min, result,
indication_count, max_indication_length_mm, max_indication_width_mm, next_inspection_due_hrs.

**findings.json**: finding_id, inspection_id, indications[ indication_no, location, type,
length_mm, width_mm, disposition ].

**ut-thickness-readings**: reading_id, inspection_id, point_ref, thickness_mm, nominal_mm, probe_id.

**calibration-records**: certificate_no, equipment_id, calibrated_on, calibration_due,
calibration_lab, result.

**inspectors.json**: inspector_id, name{{forename, surname}}, home_facility_id, employment_status,
authorization{{number, max_en4179_level, expiry}}.

## Warning

The files are deliberately imperfect, like real source data. Profile them yourself first.
A list of what was planted is in `ANSWER_KEY_planted_issues.md`. Do not open it until you
have written your own data quality checks.
""", encoding="utf-8")

(OUT / "ANSWER_KEY_planted_issues.md").write_text(
    "# Planted data problems (answer key)\n\nOpen this only after you have profiled the data yourself.\n\n"
    + "\n".join([
        f"- **Exact duplicate lines** in inspections.json ({len(key['duplicate_lines'])}): {', '.join(key['duplicate_lines'])}",
        f"- **Result casing** inconsistent (PASS, pass, Pass, FAIL, fail, Fail) on about 12 percent of rows",
        f"- **Negative duration_min** ({len(key['negative_duration'])}): {', '.join(key['negative_duration'])}. "
        f"Two of them are corrected in incremental/2025-01-13: {', '.join(key['corrected_in_batch'])}",
        f"- **Orphan component_id** not in components.csv ({len(key['orphan_component'])}): {', '.join(key['orphan_component'])}",
        f"- **Null measurements** on clean inspections ({len(key['null_measurements'])}): {', '.join(key['null_measurements'])}",
        f"- **Empty string equipment_id** instead of null ({len(key['empty_string_equipment'])}): {', '.join(key['empty_string_equipment'])}",
        f"- **Trailing space in facility_id** ({len(key['trailing_space_facility'])}): {', '.join(key['trailing_space_facility'])}",
        f"- **dd/MM/yyyy inspection_date** instead of ISO ({len(key['ddmmyyyy_date'])}): {', '.join(key['ddmmyyyy_date'])}",
        f"- **Calibration lapse**: {GAP_EQ} has no valid calibration from {GAP_START} to {GAP_END}. "
        f"Inspections done with it in that window ({len(key['calibration_lapse'])}): {', '.join(key['calibration_lapse']) or 'none'}",
        "- **\\N markers** in components.csv (serial number), workorders.csv (aircraft_reg), facilities.csv (approval ref) "
        "and ut-thickness-readings (thickness_mm)",
        f"- **Resent record**: {key.get('resent_in_batch3')} is delivered again, unchanged, in incremental/2025-01-20",
        "- **SCD2 material**: INS-002 (level 2 to 3 on 2024-06-01) and INS-007 (level 2 to 3 on 2024-10-01) in certification_history.csv; "
        "equipment calibration cycles in calibration-records/",
        "- **Mixed file shapes**: JSON lines, pretty multi-line JSON, CSV, folder of split CSVs, folder of split multi-line JSON, "
        "date-named incremental folders",
    ]) + "\n", encoding="utf-8")

print("files:", n_files)
print("inspections main lines:", len(out), "| unique ids:", len(base_by_id))
print({k: (len(v) if isinstance(v, list) else v) for k, v in key.items()})
