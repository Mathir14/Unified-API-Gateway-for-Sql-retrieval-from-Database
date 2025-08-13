import json
import os
import sqlite3
from pathlib import Path

import requests
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles


def download_from_gdrive(file_id, dest_path):
    URL = "https://docs.google.com/uc?export=download"
    session = requests.Session()

    response = session.get(URL, params={"id": file_id}, stream=True)
    token = None
    for key, value in response.cookies.items():
        if key.startswith("download_warning"):
            token = value

    if token:
        params = {"id": file_id, "confirm": token}
        response = session.get(URL, params=params, stream=True)

    with open(dest_path, "wb") as f:
        for chunk in response.iter_content(chunk_size=32768):
            if chunk:
                f.write(chunk)


# Usage
DB_PATH = "/tmp/survey_data.db"
FILE_ID = "1izuRQnlxVXblHdDjDVVkWHxPlyJt-hYQ"

if not Path(DB_PATH).exists():
    print("survey_data.db not found, downloading from Google Drive...")
    download_from_gdrive(FILE_ID, DB_PATH)
    print("Download complete.")


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

# Serve static files (if needed)
app.mount("/static", StaticFiles(directory=TEMPLATE_DIR), name="static")


# Helper: get metadata keys for a table
def get_metadata_keys(table):
    if table == "HHFV_2019-20":
        suffix = "_hh_fv"
    elif table == "HHRV_2019-20":
        suffix = "_hh_rv"
    elif table == "PERFV_2019-20":
        suffix = "_per_fv"
    elif table == "PERRV_2019-20":
        suffix = "_per_rv"
    else:
        raise ValueError("Unknown table")
    keys = [k for k in metadata.keys() if k.endswith(suffix)]
    # Sort by variable id (e.g., V33, V34, ...)
    keys.sort(key=lambda k: int(metadata[k]["id"][1:]))
    return keys


@app.get("/")
def serve_index():
    index_path = TEMPLATE_DIR / "index.html"
    return FileResponse(index_path)


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

    # Get metadata keys and labels for this table
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
