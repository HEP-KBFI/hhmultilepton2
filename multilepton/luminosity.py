"""Audit full-file skim coverage and integrate an independently produced BRIL CSV.

Run this file directly; no columnflow environment/configuration changes are made.
The input luminosity sections come from DBS, before event selections. A section
is usable only when every catalogued file containing it has a valid skim marker.
"""

from __future__ import annotations

import argparse
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
import csv
from datetime import datetime, timezone
from decimal import Decimal
import hashlib
import json
import os
from pathlib import Path
import ssl
import time
from urllib.parse import urlencode
from urllib.request import Request, urlopen


DBS = "https://cmsweb.cern.ch/dbs/prod/global/DBSReader/"


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def save(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".partial")
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")
    temporary.replace(path)


def expand(mask):
    return {(int(run), ls) for run, ranges in mask.items()
            for low, high in ranges for ls in range(int(low), int(high) + 1)}


def compact(pairs):
    result = {}
    for run, ls in sorted(set(pairs)):
        ranges = result.setdefault(str(run), [])
        if ranges and ranges[-1][1] + 1 == ls:
            ranges[-1][1] = ls
        else:
            ranges.append([ls, ls])
    return result


def complete_sections(file_sections, completed, golden):
    """Exclude a section if even one of its input files is still missing."""
    present, missing = set(), set()
    for lfn, sections in file_sections.items():
        (present if lfn in completed else missing).update(sections)
    return (present - missing) & golden, present & missing & golden


def load_completion(marker, manifest):
    import pyarrow.parquet as pq

    index = int(marker.stem)
    name, branch = manifest["entries"][index]
    row = json.loads(marker.read_text())
    parquet = Path(row["parquet"])
    expected_lfn = manifest["datasets"][name]["lfns"][branch]
    if (row["dataset"] != name or row["branch"] != branch or row["lfn"] != expected_lfn or
            not parquet.is_file() or parquet.stat().st_size != row["bytes"] or
            pq.read_metadata(parquet).num_rows != row["rows"] or
            row["rows"] != int(row["stats"]["num_events_selected"])):
        raise ValueError(f"Invalid completed skim marker: {name}/{branch}")
    return name, expected_lfn, row


def snapshot(manifests, output):
    path = output / "snapshot.json"
    if path.exists():
        cached = json.loads(path.read_text())
        expected = [{"path": str(directory / "manifest.json"), "sha256": digest(directory / "manifest.json")}
                    for directory in manifests]
        if cached["manifests"] != expected or digest(output / "golden.json") != cached["golden_sha256"]:
            raise ValueError("Snapshot inputs changed; use a new output directory")
        return cached
    result = {"started_utc": datetime.now(timezone.utc).isoformat(), "manifests": [], "datasets": {}}
    golden_hash = None
    for directory in manifests:
        source = directory / "manifest.json"
        manifest = json.loads(source.read_text())
        golden = manifest["golden"]
        if digest(golden["path"]) != golden["sha256"]:
            raise ValueError("Production Golden JSON checksum mismatch")
        if golden_hash and golden_hash != golden["sha256"]:
            raise ValueError("Input productions use different Golden JSONs")
        golden_hash = golden["sha256"]
        result["golden_sha256"] = golden_hash
        (output / "golden.json").write_bytes(Path(golden["path"]).read_bytes())
        result["manifests"].append({"path": str(source), "sha256": digest(source)})
        for name, info in manifest["datasets"].items():
            if not info["is_data"]:
                continue
            if name in result["datasets"]:
                reuse = manifest.get("reused_datasets", {}).get(name)
                previous = result["datasets"][name]
                if (not reuse or info["lfns"] != previous["lfns"] or info["keys"] != previous["keys"] or
                        {"path": str(Path(reuse["directory"]) / "manifest.json"),
                         "sha256": reuse["manifest_sha256"]} not in result["manifests"]):
                    raise ValueError(f"Repeated dataset without matching reuse provenance: {name}")
                continue
            if len(info["lfns"]) != info["n_files"] or len(set(info["lfns"])) != info["n_files"]:
                raise ValueError(f"Invalid manifest file inventory: {name}")
            result["datasets"][name] = {
                "era": name.rsplit("_", 1)[1].upper(), "keys": info["keys"],
                "lfns": info["lfns"], "completed": {},
            }
        markers = [marker for marker in sorted((directory / "completed").glob("*.json"))
                   if manifest["datasets"][manifest["entries"][int(marker.stem)][0]]["is_data"]]
        with ThreadPoolExecutor(max_workers=4) as pool:
            for name, expected_lfn, row in pool.map(lambda path: load_completion(path, manifest), markers):
                result["datasets"][name]["completed"][expected_lfn] = row
    result["finished_utc"] = datetime.now(timezone.utc).isoformat()
    save(path, result)
    return result


class Catalog:
    def __init__(self, cache):
        self.cache = cache
        self.context = ssl.create_default_context()
        self.context.load_verify_locations(capath="/cvmfs/grid.cern.ch/etc/grid-security/certificates")
        proxy = os.environ.get("X509_USER_PROXY")
        if not proxy:
            raise RuntimeError("Set X509_USER_PROXY to a valid CMS proxy before querying DBS")
        self.context.load_cert_chain(proxy)

    def query(self, endpoint, parameters, post=False):
        signature = {"endpoint": endpoint, "parameters": parameters, "post": post}
        key = hashlib.sha256(json.dumps(signature, sort_keys=True).encode()).hexdigest()
        path = self.cache / (key + ".json")
        if path.exists():
            cached = json.loads(path.read_text())
            if cached["request"] != signature:
                raise ValueError("DBS cache request mismatch")
            return cached["response"]
        url = DBS + endpoint
        body = None
        if post:
            body = json.dumps(parameters).encode()
        else:
            url += "?" + urlencode(parameters)
        for attempt in range(3):
            try:
                request = Request(url, data=body, headers={"Content-Type": "application/json"})
                with urlopen(request, context=self.context, timeout=90) as response:
                    data = json.load(response)
                if not isinstance(data, list) or not data:
                    raise ValueError("Empty or invalid DBS response")
                save(path, {"request": signature, "response": data,
                            "fetched_utc": datetime.now(timezone.utc).isoformat()})
                return data
            except Exception:
                if attempt == 2:
                    raise RuntimeError(f"DBS {endpoint} request failed; no coverage inferred") from None
                time.sleep(2)

    def file_lumis(self, lfns):
        rows = self.query("filelumis", {"logical_file_name": lfns, "validFileOnly": 1}, post=True)
        result = defaultdict(set)
        for row in rows:
            lfn = row["logical_file_name"]
            if lfn not in lfns:
                raise ValueError("DBS returned an unrequested file")
            lumis = row["lumi_section_num"]
            if isinstance(lumis, int):
                lumis = [lumis]
            result[lfn].update((int(row["run_num"]), int(ls)) for ls in lumis)
        if set(result) != set(lfns):
            raise ValueError("Missing DBS file/lumisection metadata")
        return result


def coverage(args):
    output = args.output
    output.mkdir(parents=True, exist_ok=True)
    frozen = snapshot(args.manifest, output)
    golden = expand(json.loads((output / "golden.json").read_text()))
    catalog = Catalog(output / "dbs_cache")
    report = {"snapshot_sha256": digest(output / "snapshot.json"), "datasets": {}, "common_by_era": {}}
    masks = {}
    for name, info in sorted(frozen["datasets"].items()):
        completed = set(info["completed"])
        summary = {"era": info["era"], "completed_files": len(completed), "expected_files": len(info["lfns"])}
        if not completed:
            masks[name] = set()
            summary.update(complete_lumisections=0, partial_lumisections=0, catalog_checked=False)
        else:
            live_files = set()
            for key in info["keys"]:
                rows = catalog.query("files", {"dataset": key, "validFileOnly": 1, "detail": 1})
                for row in rows:
                    if row["is_file_valid"] != 1:
                        raise ValueError("DBS returned an invalid file")
                    lfn = row["logical_file_name"]
                    live_files.add(lfn)
                    if lfn in completed and int(row["event_count"]) != int(
                        info["completed"][lfn]["stats"]["num_events"],
                    ):
                        raise ValueError(f"Raw event count mismatch: {name}")
            if live_files != set(info["lfns"]):
                raise ValueError(f"Production file inventory differs from current DBS: {name}")
            batches = [info["lfns"][i:i + 100] for i in range(0, len(info["lfns"]), 100)]
            all_lumis = {}
            with ThreadPoolExecutor(max_workers=args.workers) as pool:
                for mapping in pool.map(catalog.file_lumis, batches):
                    all_lumis.update(mapping)
            usable, partial = complete_sections(all_lumis, completed, golden)
            masks[name] = usable
            summary.update(complete_lumisections=len(usable), partial_lumisections=len(partial), catalog_checked=True)
            save(output / "masks" / f"{name}_partial_excluded.json", compact(partial))
        save(output / "masks" / f"{name}.json", compact(masks[name]))
        report["datasets"][name] = summary
        print(name, summary, flush=True)
    all_common = set()
    for era in sorted({d["era"] for d in report["datasets"].values()}):
        names = [name for name, d in report["datasets"].items() if d["era"] == era]
        if len(names) != 4:
            raise ValueError(f"Expected electron, muon, MuonEG and tau collections in era {era}")
        common = set.intersection(*(masks[name] for name in names))
        if all_common & common:
            raise ValueError("Overlapping data-taking eras")
        all_common.update(common)
        report["common_by_era"][era] = {"datasets": names, "complete_lumisections": len(common)}
        save(output / "masks" / f"common_{era}.json", compact(common))
    save(output / "masks" / "common_all.json", compact(all_common))
    # A single BRIL query can supply every per-dataset and common-mask integral.
    save(output / "bril_input.json", compact(set.union(*masks.values())))
    report["notes"] = [
        "Masks describe complete input coverage, before event cuts.",
        "Apply the chosen mask to the data before using its luminosity for MC normalization.",
        "Per-dataset luminosities overlap and must not be added.",
        "Common masks require all four analysis collections; dataset overlap removal is still required.",
        "Recorded luminosity is not a validation of trigger prescales or the channel trigger OR.",
    ]
    save(output / "coverage.json", report)


def read_bril(path):
    rows, columns = {}, None
    with Path(path).open() as stream:
        for line in stream:
            if line.startswith("#run:fill,"):
                columns = next(csv.reader([line.lstrip("#")]))
                if "recorded(/pb)" not in columns:
                    raise ValueError("BRIL CSV must use /pb units")
            elif line.strip() and not line.startswith("#") and columns:
                values = next(csv.reader([line]))
                if len(values) != len(columns):
                    raise ValueError("Unexpected BRIL CSV row")
                row = dict(zip(columns, values))
                key = (int(row["run:fill"].split(":")[0]), int(row["ls"].split(":")[0]))
                value = Decimal(row["recorded(/pb)"])
                if key in rows or not value.is_finite():
                    raise ValueError("Duplicate or invalid BRIL luminosity row")
                rows[key] = value
    if not columns or not rows:
        raise ValueError("BRIL CSV has no luminosity rows")
    return rows


def summarize(args):
    output = args.output
    report = json.loads((output / "coverage.json").read_text())
    rows = read_bril(args.csv)
    requested = expand(json.loads((output / "bril_input.json").read_text()))
    query_mask = args.bril_input or output / "bril_input.json"
    queried = expand(json.loads(query_mask.read_text()))
    if not requested <= queried:
        raise ValueError("BRIL input does not cover the requested masks")
    if set(rows) - queried:
        raise ValueError("BRIL CSV contains unrequested lumisections")
    report["bril"] = {"csv": str(args.csv.resolve()), "csv_sha256": digest(args.csv),
                      "input": str(query_mask.resolve()), "input_sha256": digest(query_mask),
                      "normtag_sha256": digest(args.normtag), "rows": len(rows),
                      "missing_lumisections": len(requested - set(rows))}
    report["luminosity"] = {}
    for path in sorted((output / "masks").glob("*.json")):
        if "partial_excluded" in path.stem:
            continue
        wanted = expand(json.loads(path.read_text()))
        missing = wanted - set(rows)
        negative = {key for key in wanted & set(rows) if rows[key] < 0}
        value = sum((rows[key] for key in wanted & set(rows)), Decimal(0))
        usable = wanted - missing - negative
        usable_value = sum((rows[key] for key in usable), Decimal(0))
        usable_path = output / "usable_masks" / path.name
        save(usable_path, compact(usable))
        report["luminosity"][path.stem] = {
            "mask": str(path.resolve()), "mask_sha256": digest(path), "lumisections": len(wanted),
            "missing_bril_lumisections": len(missing),
            "negative_bril_lumisections": len(negative),
            "recorded_pb": float(value) if not missing and not negative else None,
            "recorded_fb": float(value / 1000) if not missing and not negative else None,
            "covered_recorded_pb": float(value),
            "usable_mask": str(usable_path.resolve()), "usable_mask_sha256": digest(usable_path),
            "usable_lumisections": len(usable), "usable_recorded_pb": float(usable_value),
            "usable_recorded_fb": float(usable_value / 1000),
            "usage": "Apply usable_mask to data; usable_recorded_pb is only valid for that exact subset.",
        }
    save(output / "luminosity.json", report)
    save(output / "missing_bril_lumis.json", compact(requested - set(rows)))
    save(output / "negative_bril_lumis.json", compact(key for key in requested & set(rows) if rows[key] < 0))
    print(json.dumps(report["luminosity"], indent=2))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    collect = sub.add_parser("coverage")
    collect.add_argument("--manifest", type=Path, action="append", required=True)
    collect.add_argument("--output", type=Path, required=True)
    collect.add_argument("--workers", type=int, default=3)
    integrate = sub.add_parser("summarize")
    integrate.add_argument("--output", type=Path, required=True)
    integrate.add_argument("--csv", type=Path, required=True)
    integrate.add_argument("--normtag", type=Path, required=True)
    integrate.add_argument("--bril-input", type=Path, help="Exact BRIL query mask, if broader than skim coverage")
    args = parser.parse_args()
    if args.command == "coverage":
        if not 1 <= args.workers <= 4:
            parser.error("Use between one and four catalogue workers")
        coverage(args)
    else:
        summarize(args)


if __name__ == "__main__":
    main()
