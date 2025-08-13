import json
import sqlite3
from pathlib import Path

import requests
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles


# ---------- Dropbox download ----------
def download_from_dropbox(url: str, dest_path: str):
    # Force direct download
    if url.endswith("?dl=0"):
        url = url[:-5] + "?dl=1"

    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(dest_path, "wb") as f:
            for chunk in r.iter_content(32768):
                if chunk:
                    f.write(chunk)

    # Sanity check
    with open(dest_path, "rb") as f:
        header = f.read(16)
        if not header.startswith(b"SQLite format 3"):
            raise RuntimeError("Downloaded file is not a valid SQLite database")


# ---------- Setup database ----------
DB_PATH = "/tmp/survey_data.db"
DROPBOX_URL = "https://www.dropbox.com/scl/fi/9y1fwira5jpoip4lzh5a4/survey_data.db?rlkey=phr69jvzudrok1k0u2sdltwhd&st=bys086nx&dl=1"

if not Path(DB_PATH).exists():
    print("survey_data.db not found, downloading from Dropbox...")
    download_from_dropbox(DROPBOX_URL, DB_PATH)
    print("Download complete!")


# ---------- FastAPI setup ----------
app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

BASE_DIR = Path(__file__).resolve().parent.parent
METADATA_PATH = BASE_DIR / "metadata.json"
TEMPLATE_DIR = BASE_DIR / "template"

allowed_tables = {"HHFV_2019-20", "HHRV_2019-20", "PERFV_2019-20", "PERRV_2019-20"}

with open(METADATA_PATH, "r", encoding="utf-8") as f:
    metadata = json.load(f)

# Serve static files
app.mount("/static", StaticFiles(directory=TEMPLATE_DIR), name="static")


# ---------- Helper ----------
def get_metadata_keys(table):
    suffix_map = {
        "HHFV_2019-20": "_hh_fv",
        "HHRV_2019-20": "_hh_rv",
        "PERFV_2019-20": "_per_fv",
        "PERRV_2019-20": "_per_rv",
    }
    if table not in suffix_map:
        raise ValueError("Unknown table")
    suffix = suffix_map[table]
    keys = [k for k in metadata.keys() if k.endswith(suffix)]
    keys.sort(key=lambda k: int(metadata[k]["id"][1:]))
    return keys


# ---------- Routes ----------
@app.get("/")
def serve_index():
    return FileResponse(TEMPLATE_DIR / "index.html")


@app.get("/api/{table_name}/data")
def get_table_data(
    table_name: str,
    limit: int = Query(
        default=None, description="Number of rows to return; all if not set"
    ),
    filter_col: str = None,
    filter_val: str = None,
):
    table_name = table_name.upper()
    if table_name not in allowed_tables:
        raise HTTPException(status_code=404, detail="Table not found")

    meta_keys = get_metadata_keys(table_name)
    labels = [metadata[k]["label"] for k in meta_keys]
    categories_list = [metadata[k].get("categories", {}) for k in meta_keys]

    query = f'SELECT * FROM "{table_name}"'
    params = []
    if filter_col and filter_val:
        query += f' WHERE "{filter_col}" = ?'
        params.append(filter_val)
    if limit is not None:
        query += f" LIMIT {limit}"

    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.execute(query, params)
        rows = cursor.fetchall()

    result = []
    for row in rows:
        item = {}
        for idx, val in enumerate(row):
            label = labels[idx]
            categories = categories_list[idx]
            display_val = categories.get(str(val), val) if categories else val
            item[label] = display_val
        result.append(item)

    return {"data": result}
