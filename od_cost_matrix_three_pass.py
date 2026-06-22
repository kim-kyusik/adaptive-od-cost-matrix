"""
Three-Pass Origin-Destination Cost Matrix
=========================================

Computes network travel times and distances from origin block groups to
NAICS-coded point-of-interest (POI) destinations using ArcGIS Network Analyst
(``arcpy.nax``), with an adaptive three-pass buffering strategy:

    Pass 1: per-origin county-buffer = 300 miles (standard travel mode)
    Pass 2: county-buffer = 500 miles, only for origins that found NO
            destination in Pass 1
    Pass 3: county-buffer = 1000 miles, with the "Driving an Automobile"
            restriction removed and hierarchy disabled, only for origins
            still unmatched after Pass 2

Origins are processed in county-grouped chunks. For each chunk, destinations
are pre-selected within a buffer around the chunk's mean origin location, which
keeps each OD solve tractable. Results are written to per-chunk Parquet files,
so the run is checkpointed and resumable: existing chunk outputs are skipped.

Configuration (data paths, NAICS code, buffer distances, parallelism) is read
from a YAML file. Copy ``config.example.yaml`` to ``config.yaml`` and edit it,
or point the OD_CONFIG environment variable at an alternate config path.

Requirements: ArcGIS Pro with the Network Analyst extension (arcpy), a routing
network dataset (e.g., Esri StreetMap Premium), pandas, pyarrow, and PyYAML.
See README.md for the full requirements and data-availability notes.
"""

__version__ = "1.0.0"

import arcpy
import arcpy.nax as nax
import pandas as pd
import os
import glob
import yaml
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime
from pathlib import Path

# ----------------------------------------------------------------------------------------
# CONFIGURATION
# ----------------------------------------------------------------------------------------
# All machine-specific and run-specific settings live in a YAML config file so
# that no local paths are hard-coded in this script. See config.example.yaml.
#
# NAICS codes this pipeline has been used with (set `naics_code` in the config):
#   621320, 621340, 621492, 621493, 621498, 621511, 621512,
#   621111, 621420, 622110, 622310, 812191, 446110
# Note: very large destination sets (e.g., 621111) may need a larger chunk_size
# and/or fewer workers to stay within memory.

CONFIG_PATH = os.environ.get("OD_CONFIG", "config.yaml")
with open(CONFIG_PATH, "r", encoding="utf-8") as _f:
    _cfg = yaml.safe_load(_f)

# --- Settings loaded from config ---
NAICS_CODE = str(_cfg["naics_code"])
SRC_GDB = _cfg["src_gdb"]                       # file geodatabase holding origins + POIs
NDS = _cfg["network_dataset"]                   # routing network dataset path
TRAVEL_MODE_NAME = _cfg.get("travel_mode_name", "Driving Time")
SEARCH_TOLERANCE = _cfg.get("search_tolerance", 7000)
CHUNK_SIZE = _cfg.get("chunk_size", 500)
MAX_WORKERS = _cfg.get("max_workers", 14)
BUFFER_DIST_PASS1 = _cfg.get("buffer_dist_pass1", "300 Miles")
BUFFER_DIST_PASS2 = _cfg.get("buffer_dist_pass2", "500 Miles")
BUFFER_DIST_PASS3 = _cfg.get("buffer_dist_pass3", "1000 Miles")

arcpy.env.overwriteOutput = True

# --- Field names (tied to the input data schema) ---
ORIG_ID_NAME = "Name"
DEST_ID_NAME = "Name"

# --- Derived feature classes ---
ORIG_FC = fr"{SRC_GDB}/origins_bg"
DEST_FC = fr"{SRC_GDB}/poi_advan_{NAICS_CODE}"

# --- Output directories per pass ---
OUT_DIR_PASS1 = fr"travel_time_nax_{NAICS_CODE}_pass1_{BUFFER_DIST_PASS1.replace(' ', '').lower()}"
OUT_DIR_PASS2 = fr"travel_time_nax_{NAICS_CODE}_pass2_{BUFFER_DIST_PASS2.replace(' ', '').lower()}"
OUT_DIR_PASS3 = fr"travel_time_nax_{NAICS_CODE}_pass3_{BUFFER_DIST_PASS3.replace(' ', '').lower()}_no_restriction"

# --- Files to store unmatched origin IDs between passes ---
UNMATCHED_ORIGINS_P1_FILE = fr"unmatched_origins_{NAICS_CODE}_pass1.txt"
UNMATCHED_ORIGINS_P2_FILE = fr"unmatched_origins_{NAICS_CODE}_pass2.txt"


# ----------------------------------------------------------------------------------------
# CORE PROCESSING FUNCTION (county subset + chunk solve)
# ----------------------------------------------------------------------------------------
def run_od_analysis(
    pass_label,
    chunk_index,
    total_chunks,
    chunk_key,
    orig_county,
    origin_ids,
    dest_fc,
    nds,
    out_dir,
    buffer_dist,
    remove_driving_restriction=False,
):
    import arcpy
    import arcpy.nax as nax
    import pandas as pd
    import os

    out_file = os.path.join(out_dir, f"od_{chunk_key}.parquet")
    if os.path.exists(out_file):
        return (
            f"[{pass_label}] Skip {chunk_index + 1}/{total_chunks} | "
            f"chunk={chunk_key} already exists."
        )

    orig_lyr = None
    dest_lyr = None
    orig_sel = None
    dest_sel = None
    mean_pt_fc = None
    buf_fc = None

    try:
        arcpy.env.overwriteOutput = True

        odcm = nax.OriginDestinationCostMatrix(nds)
        travel_modes = nax.GetTravelModes(nds)
        selected_mode = travel_modes[TRAVEL_MODE_NAME]
        selected_mode.useHierarchy = "USE_HIERARCHY"

        if remove_driving_restriction:
            selected_mode.useHierarchy = "NO_HIERARCHY"
            original_restrictions = list(selected_mode.restrictions)
            selected_mode.restrictions = [
                r for r in original_restrictions
                if r != "Driving an Automobile"
            ]

        odcm.travelMode = selected_mode
        odcm.distanceUnits = nax.DistanceUnits.Kilometers
        odcm.searchTolerance = SEARCH_TOLERANCE
        odcm.searchToleranceUnits = arcpy.nax.DistanceUnits.Meters
        odcm.allowAutoRelocate = True
        odcm.lineShapeType = nax.LineShapeType.NoLine
        odcm.accumulateAttributeNames = ["TravelTime", "Kilometers"]

        safe_ids = [str(x).replace("'", "''") for x in origin_ids]
        id_list_str = ",".join([f"'{x}'" for x in safe_ids])
        where = f"{ORIG_ID_NAME} IN ({id_list_str})"

        orig_lyr = arcpy.management.MakeFeatureLayer(
            ORIG_FC, f"orig_lyr_{chunk_key}", where
        ).getOutput(0)
        dest_lyr = arcpy.management.MakeFeatureLayer(
            dest_fc, f"dest_lyr_{chunk_key}"
        ).getOutput(0)

        orig_sel = arcpy.management.CopyFeatures(
            orig_lyr, fr"in_memory/orig_sel_{chunk_key}"
        ).getOutput(0)
        origin_count = int(arcpy.management.GetCount(orig_sel).getOutput(0))
        if origin_count == 0:
            return (
                f"[{pass_label}] Skip {chunk_index + 1}/{total_chunks} | "
                f"chunk={chunk_key} has no selected origins."
            )

        sr = arcpy.Describe(orig_sel).spatialReference
        coords = [row[0] for row in arcpy.da.SearchCursor(orig_sel, ["SHAPE@XY"])]
        if not coords:
            return (
                f"[{pass_label}] Skip {chunk_index + 1}/{total_chunks} | "
                f"chunk={chunk_key} has no origin geometry."
            )

        xs, ys = zip(*coords)
        mean_point = arcpy.PointGeometry(
            arcpy.Point(sum(xs) / len(xs), sum(ys) / len(ys)),
            sr,
        )
        mean_pt_fc = fr"in_memory/mean_pt_{chunk_key}"
        arcpy.management.CopyFeatures([mean_point], mean_pt_fc)
        buf_fc = arcpy.analysis.Buffer(
            mean_pt_fc, fr"in_memory/mean_buf_{chunk_key}", buffer_dist
        ).getOutput(0)

        arcpy.management.SelectLayerByLocation(
            in_layer=dest_lyr,
            overlap_type="INTERSECT",
            select_features=buf_fc,
            selection_type="NEW_SELECTION",
        )
        dest_sel = arcpy.management.CopyFeatures(
            dest_lyr, fr"in_memory/dest_sel_{chunk_key}"
        ).getOutput(0)
        dest_count = int(arcpy.management.GetCount(dest_sel).getOutput(0))
        arcpy.management.SelectLayerByAttribute(dest_lyr, "CLEAR_SELECTION")

        if dest_count == 0:
            pd.DataFrame(
                columns=["OriginName", "DestinationName", "Total_Time_Min", "Total_Distance_km"]
            ).to_parquet(out_file, index=False, compression="snappy")
            return (
                f"[{pass_label}] Done {chunk_index + 1}/{total_chunks} | "
                f"chunk={chunk_key} | county={orig_county} | rows=0 | no destinations in buffer"
            )

        print(
            f"[{pass_label}] Solving {chunk_index + 1}/{total_chunks} | "
            f"chunk={chunk_key} | county={orig_county} | "
            f"{origin_count} origins | {dest_count} destinations | "
            f"buffer={buffer_dist}"
        )

        ofm = odcm.fieldMappings(nax.OriginDestinationCostMatrixInputDataType.Origins)
        ofm["Name"].mappedFieldName = ORIG_ID_NAME
        dfm = odcm.fieldMappings(nax.OriginDestinationCostMatrixInputDataType.Destinations)
        dfm["Name"].mappedFieldName = DEST_ID_NAME

        odcm.load(nax.OriginDestinationCostMatrixInputDataType.Origins, orig_sel, ofm)
        odcm.load(nax.OriginDestinationCostMatrixInputDataType.Destinations, dest_sel, dfm)

        result = odcm.solve()

        if result.solveSucceeded:
            fields = ["OriginName", "DestinationName", "Total_Time", "Total_Distance"]
            with result.searchCursor(
                nax.OriginDestinationCostMatrixOutputDataType.Lines, fields
            ) as cur:
                data = [row for row in cur]

            if data:
                df = pd.DataFrame(data, columns=fields)
                df["Total_Time"] = df["Total_Time"].astype("float32")
                df["Total_Distance"] = df["Total_Distance"].astype("float32")
                df = df.rename(columns={
                    "Total_Time": "Total_Time_Min",
                    "Total_Distance": "Total_Distance_km",
                })
                df.to_parquet(out_file, index=False, compression="snappy")
                return (
                    f"[{pass_label}] Done {chunk_index + 1}/{total_chunks} | "
                    f"chunk={chunk_key} | county={orig_county} | rows={len(df)}"
                )

            pd.DataFrame(
                columns=["OriginName", "DestinationName", "Total_Time_Min", "Total_Distance_km"]
            ).to_parquet(out_file, index=False, compression="snappy")
            return (
                f"[{pass_label}] Done {chunk_index + 1}/{total_chunks} | "
                f"chunk={chunk_key} | county={orig_county} | rows=0"
            )

        messages = result.solverMessages(nax.MessageSeverity.Error)
        return (
            f"[{pass_label}] Fail {chunk_index + 1}/{total_chunks} | "
            f"chunk={chunk_key} | county={orig_county} | {messages}"
        )

    except Exception as e:
        return (
            f"[{pass_label}] Error {chunk_index + 1}/{total_chunks} | "
            f"chunk={chunk_key} | county={orig_county} | {e}"
        )

    finally:
        if orig_lyr is not None:
            arcpy.management.Delete(orig_lyr)
        if dest_lyr is not None:
            arcpy.management.Delete(dest_lyr)
        if orig_sel is not None:
            arcpy.management.Delete(orig_sel)
        if dest_sel is not None:
            arcpy.management.Delete(dest_sel)
        if mean_pt_fc is not None:
            arcpy.management.Delete(mean_pt_fc)
        if buf_fc is not None:
            arcpy.management.Delete(buf_fc)


# ----------------------------------------------------------------------------------------
# HELPER: identify unmatched origins from a pass's results
# ----------------------------------------------------------------------------------------
def find_unmatched_origins(out_dir, candidate_origin_ids):
    """
    Read all parquet chunks from out_dir, collect unique OriginName values,
    and return the set of origin IDs that never appeared in any result row.
    """
    parquet_files = sorted(glob.glob(os.path.join(out_dir, "od_*.parquet")))
    if not parquet_files:
        print("WARNING: No parquet files found — all origins are unmatched.")
        return set(candidate_origin_ids)

    matched = set()
    for pf in parquet_files:
        df = pd.read_parquet(pf, columns=["OriginName"])
        matched.update(df["OriginName"].unique())

    unmatched = set(candidate_origin_ids) - matched
    return unmatched


# ----------------------------------------------------------------------------------------
# HELPER: build county-aware chunk tasks
# ----------------------------------------------------------------------------------------
def origin_to_county(origin_id):
    return str(origin_id)[:5]


def make_county_chunks(origin_ids, chunk_size):
    county_to_origins = {}
    for origin_id in origin_ids:
        county = origin_to_county(origin_id)
        county_to_origins.setdefault(county, []).append(origin_id)

    chunks = []
    seq = 0
    for county in sorted(county_to_origins):
        county_origins = sorted(county_to_origins[county])
        for idx, start in enumerate(range(0, len(county_origins), chunk_size)):
            chunk_key = f"{county}_{idx:04d}"
            chunk_origin_ids = county_origins[start:start + chunk_size]
            chunks.append((seq, chunk_key, county, chunk_origin_ids))
            seq += 1

    return chunks


# ----------------------------------------------------------------------------------------
# HELPER: run a pass (with checkpoint support)
# ----------------------------------------------------------------------------------------
def run_pass(pass_label, out_dir, chunks, dest_fc, nds, buffer_dist,
             max_workers=MAX_WORKERS, remove_driving_restriction=False):
    os.makedirs(out_dir, exist_ok=True)

    existing = glob.glob(os.path.join(out_dir, "od_*.parquet"))
    done_ids = {Path(f).stem.removeprefix("od_") for f in existing}

    tasks = [c for c in chunks if c[1] not in done_ids]

    restriction_note = " | restrictions relaxed" if remove_driving_restriction else ""
    print(f"\n{'='*60}")
    print(f"[{pass_label}] buffer={buffer_dist}{restriction_note} | "
          f"chunks: {len(chunks)} total, {len(done_ids)} done, {len(tasks)} remaining")
    print(f"{'='*60}")

    if not tasks:
        print(f"[{pass_label}] All chunks already complete.")
        return

    total_chunks = len(chunks)

    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(
                run_od_analysis,
                pass_label,
                chunk_index,
                total_chunks,
                chunk_key,
                county,
                cdata,
                dest_fc,
                nds,
                out_dir,
                buffer_dist,
                remove_driving_restriction,
            )
            for chunk_index, chunk_key, county, cdata in tasks
        ]

        for fut in futures:
            print(fut.result())


# ----------------------------------------------------------------------------------------
# SUMMARY LOG
# ----------------------------------------------------------------------------------------
def append_naics_summary(md_path, naics_code, total_origins,
                         unmatched_p1, unmatched_p2, unmatched_p3,
                         start_time, end_time):
    md_path = Path(md_path)
    elapsed = end_time - start_time
    if not md_path.exists():
        md_path.parent.mkdir(parents=True, exist_ok=True)
        md_path.write_text(
            "# NAICS Three-Pass Run Summary\n\n"
            "| Run Time | NAICS | Total Origins | Unmatched P1→P2 | Unmatched P2→P3 | Still Unmatched | Elapsed |\n"
            "|---|---:|---:|---:|---:|---:|---|\n",
            encoding="utf-8",
        )
    with md_path.open("a", encoding="utf-8") as f:
        f.write(
            f"| {datetime.now():%Y-%m-%d %H:%M:%S} "
            f"| {naics_code} "
            f"| {total_origins:,} "
            f"| {unmatched_p1:,} "
            f"| {unmatched_p2:,} "
            f"| {unmatched_p3:,} "
            f"| {elapsed} |\n"
        )


# ----------------------------------------------------------------------------------------
# MAIN
# ----------------------------------------------------------------------------------------
if __name__ == "__main__":
    start_time = datetime.now()
    print(f"=== Three-Pass OD Analysis for NAICS {NAICS_CODE} ===")
    print(f"Start: {start_time}")

    # ---- Collect all origin IDs ----
    all_origin_ids = [str(row[0]) for row in arcpy.da.SearchCursor(ORIG_FC, [ORIG_ID_NAME])]
    total_origins = len(all_origin_ids)
    print(f"Total origins: {total_origins:,}")

    # ================================================================
    # PASS 1: buffer = 300 miles (standard)
    # ================================================================
    chunks_pass1 = make_county_chunks(all_origin_ids, CHUNK_SIZE)
    run_pass("PASS 1", OUT_DIR_PASS1, chunks_pass1, DEST_FC, NDS, BUFFER_DIST_PASS1)

    # ---- Identify unmatched origins after Pass 1 ----
    print("\n--- Identifying unmatched origins from Pass 1 ---")
    unmatched_p1 = find_unmatched_origins(OUT_DIR_PASS1, all_origin_ids)
    unmatched_p1_list = sorted(unmatched_p1)
    print(f"Unmatched after Pass 1: {len(unmatched_p1_list):,} / {total_origins:,} "
          f"({len(unmatched_p1_list)/total_origins*100:.1f}%)")

    with open(UNMATCHED_ORIGINS_P1_FILE, "w") as f:
        f.write("\n".join(unmatched_p1_list))
    print(f"Saved → {UNMATCHED_ORIGINS_P1_FILE}")

    # ================================================================
    # PASS 2: buffer = 500 miles (only unmatched from Pass 1)
    # ================================================================
    if unmatched_p1_list:
        chunks_pass2 = make_county_chunks(unmatched_p1_list, CHUNK_SIZE)
        run_pass("PASS 2", OUT_DIR_PASS2, chunks_pass2, DEST_FC, NDS, BUFFER_DIST_PASS2)

        # ---- Identify unmatched origins after Pass 2 ----
        print("\n--- Identifying unmatched origins from Pass 2 ---")
        unmatched_p2 = find_unmatched_origins(OUT_DIR_PASS2, unmatched_p1_list)
        unmatched_p2_list = sorted(unmatched_p2)
        print(f"Unmatched after Pass 2: {len(unmatched_p2_list):,} / {len(unmatched_p1_list):,} "
              f"({len(unmatched_p2_list)/max(len(unmatched_p1_list),1)*100:.1f}%)")

        with open(UNMATCHED_ORIGINS_P2_FILE, "w") as f:
            f.write("\n".join(unmatched_p2_list))
        print(f"Saved → {UNMATCHED_ORIGINS_P2_FILE}")
    else:
        print("\nNo unmatched origins after Pass 1 — Pass 2 skipped.")
        unmatched_p2_list = []

    # ================================================================
    # PASS 3: buffer = 1000 miles + remove "Driving an Automobile" restriction
    #         (only unmatched from Pass 2)
    # ================================================================
    if unmatched_p2_list:
        chunks_pass3 = make_county_chunks(unmatched_p2_list, CHUNK_SIZE)
        run_pass(
            "PASS 3", OUT_DIR_PASS3, chunks_pass3, DEST_FC, NDS, BUFFER_DIST_PASS3,
            remove_driving_restriction=True
        )

        # ---- Check what's still unmatched after Pass 3 ----
        still_unmatched = find_unmatched_origins(OUT_DIR_PASS3, unmatched_p2_list)
        still_unmatched_list = sorted(still_unmatched)
        print(f"\nStill unmatched after Pass 3: {len(still_unmatched_list):,}")
        if still_unmatched_list:
            still_file = fr"unmatched_origins_{NAICS_CODE}_pass3_final.txt"
            with open(still_file, "w") as f:
                f.write("\n".join(still_unmatched_list))
            print(f"Saved → {still_file}")
    else:
        print("\nNo unmatched origins after Pass 2 — Pass 3 skipped.")
        still_unmatched_list = []

    # ---- Summary ----
    end_time = datetime.now()
    print(f"\n{'='*60}")
    print(f"=== FINAL SUMMARY for NAICS {NAICS_CODE} ===")
    print(f"Total origins:           {total_origins:,}")
    print(f"Unmatched after Pass 1:  {len(unmatched_p1_list):,}")
    print(f"Unmatched after Pass 2:  {len(unmatched_p2_list):,}")
    print(f"Unmatched after Pass 3:  {len(still_unmatched_list):,}")
    print(f"Total elapsed:           {end_time - start_time}")
    print(f"{'='*60}")

    append_naics_summary(
        md_path="nax_summary.md",
        naics_code=NAICS_CODE,
        total_origins=total_origins,
        unmatched_p1=len(unmatched_p1_list),
        unmatched_p2=len(unmatched_p2_list),
        unmatched_p3=len(still_unmatched_list),
        start_time=start_time,
        end_time=end_time,
    )
