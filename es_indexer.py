"""
es_indexer.py
─────────────────────────────────────────────────────────────
Reads sample_data.xls (CSV-format) and bulk-indexes two indexes:
  • nermal_text         – text chunks per PROCNR
  • teamcenter_files    – attachment metadata per PROCNR

Usage:
    python es_indexer.py
    python es_indexer.py --recreate          # drop & re-create indexes
    python es_indexer.py --file my_data.csv  # custom input file
"""

import ast
import hashlib
import json
import urllib3
import warnings
import argparse
import requests
import pandas as pd
from datetime import datetime

warnings.filterwarnings("ignore")
urllib3.disable_warnings()


# ══════════════════════════════════════════════════════════════
# CONFIG
# ══════════════════════════════════════════════════════════════

ES_HOST     = "https://muclv0688.muc.infineon.com:9200"
ES_USERNAME = "SINqmagent"
ES_PASSWORD = "aGentiC4I@25-12"

NERMAL_TEXT_INDEX      = "agentic_ai-nermal_text"
TEAMCENTER_FILES_INDEX = "agentic_ai-teamcenter_files"

TEXT_COLUMNS = ["TITLE", "MOTIVATION", "CURRENT_PROCEDURE", "NEWPROCEDURE", "REMARK"]

# ── Index Mappings ────────────────────────────────────────────

NERMAL_TEXT_MAPPING = {
    "settings": {
        "number_of_shards":   1,
        "number_of_replicas": 0,
        "refresh_interval":   "30s",
    },
    "mappings": {
        "properties": {
            "id":             {"type": "keyword"},
            "PROCNR":         {"type": "keyword"},
            "chunk_seq":      {"type": "integer"},
            "content":        {"type": "text", "analyzer": "standard"},
            "embedding":      {
                "type":       "dense_vector",
                "dims":       768,
                "index":      True,
                "similarity": "cosine",
            },
            "source_columns": {"type": "keyword"},
            "indexed_at":     {"type": "date"},
        }
    },
}

TEAMCENTER_FILES_MAPPING = {
    "settings": {
        "number_of_shards":   1,
        "number_of_replicas": 0,
        "refresh_interval":   "30s",
    },
    "mappings": {
        "properties": {
            "id":         {"type": "keyword"},
            "PROCNR":     {"type": "keyword"},
            "filename":   {
                "type":   "keyword",
                "fields": {"text": {"type": "text"}},
            },
            "filepath":   {"type": "keyword"},
            "filetype":   {"type": "keyword"},
            "doctype":    {"type": "keyword"},
            "chunk_seq":  {"type": "integer"},
            "content":    {"type": "text", "analyzer": "standard"},
            "embedding":  {
                "type":       "dense_vector",
                "dims":       768,
                "index":      True,
                "similarity": "cosine",
            },
            "indexed_at": {"type": "date"},
            "orig_obid":  {"type": "keyword"},
        }
    },
}


# ══════════════════════════════════════════════════════════════
# ELASTICSEARCH CLIENT
# ══════════════════════════════════════════════════════════════

class ElasticsearchClient:
    """Thin wrapper around requests for Elasticsearch REST calls."""

    JSON_HEADERS   = {"Content-Type": "application/json",       "Accept": "application/json"}
    NDJSON_HEADERS = {"Content-Type": "application/x-ndjson",   "Accept": "application/json"}

    def __init__(self, host: str, username: str, password: str, verify_ssl: bool = False):
        self.host      = host.rstrip("/")
        self.auth      = (username, password)
        self.verify    = verify_ssl

    # ── Private helpers ───────────────────────────────────────

    def _url(self, path: str) -> str:
        return f"{self.host}/{path.lstrip('/')}"

    def _request(self, method: str, path: str, body=None,
                 is_ndjson: bool = False, timeout: int = 30):
        url     = self._url(path)
        headers = self.NDJSON_HEADERS if is_ndjson else self.JSON_HEADERS
        try:
            return requests.request(
                method, url,
                auth    = self.auth,
                headers = headers,
                data    = body if is_ndjson else None,
                json    = body if not is_ndjson else None,
                verify  = self.verify,
                timeout = timeout,
            )
        except Exception as exc:
            print(f"  ❌ [{method}] {path}: {exc}")
            return None

    # ── Public API ────────────────────────────────────────────

    def head(self, path: str):
        return self._request("HEAD", path, timeout=10)

    def get(self, path: str):
        return self._request("GET", path, timeout=10)

    def put(self, path: str, body: dict):
        return self._request("PUT", path, body=body)

    def post(self, path: str, body: dict):
        return self._request("POST", path, body=body)

    def post_ndjson(self, path: str, payload: str):
        return self._request("POST", path, body=payload, is_ndjson=True, timeout=60)

    def delete(self, path: str):
        return self._request("DELETE", path, timeout=10)

    def ping(self) -> bool:
        res = self.get("/")
        return res is not None and res.status_code == 200


# ══════════════════════════════════════════════════════════════
# INDEXER
# ══════════════════════════════════════════════════════════════

class ESIndexer:
    """
    Handles index creation and bulk ingestion for nermal_text
    and teamcenter_files indexes.
    """

    def __init__(self, client: ElasticsearchClient):
        self.es = client

    # ── Helpers ───────────────────────────────────────────────

    @staticmethod
    def _make_id(*parts) -> str:
        """MD5 hash of joined string parts → deterministic doc id."""
        raw = "-".join(str(p) for p in parts)
        return hashlib.md5(raw.encode()).hexdigest()

    @staticmethod
    def _clean(value) -> str | None:
        """Return stripped string or None for NaN / empty."""
        if pd.isna(value) or str(value).strip() in ("", "nan", "None", "-"):
            return None
        return str(value).strip()

    # ── Index management ──────────────────────────────────────

    def create_index(self, index_name: str, mapping: dict, recreate: bool = False) -> bool:
        """Create ES index; optionally delete first if recreate=True."""
        res    = self.es.head(index_name)
        exists = res is not None and res.status_code == 200

        if exists and recreate:
            del_res = self.es.delete(index_name)
            if del_res and del_res.status_code == 200:
                print(f"  🗑️  Deleted  : {index_name}")
                exists = False
            else:
                print(f"  ❌ Delete failed for {index_name}")
                return False

        if not exists:
            res = self.es.put(index_name, mapping)
            if res and res.status_code in (200, 201):
                print(f"  ✅ Created  : {index_name}")
                return True
            err = (res.json().get("error", {}).get("reason", "") if res else "No response")
            print(f"  ❌ Failed   : {index_name} → {err}")
            return False

        print(f"  ℹ️  Exists   : {index_name}")
        return True

    def setup_indexes(self, recreate: bool = False):
        """Create both indexes."""
        print("\n" + "=" * 55)
        print("  Creating / Verifying Indexes")
        print("=" * 55)
        self.create_index(NERMAL_TEXT_INDEX,      NERMAL_TEXT_MAPPING,      recreate)
        self.create_index(TEAMCENTER_FILES_INDEX, TEAMCENTER_FILES_MAPPING, recreate)

    # ── Document preparation ──────────────────────────────────

    def prepare_nermal_docs(self, df: pd.DataFrame) -> list[dict]:
        """
        Build nermal_text documents from each TEXT_COLUMN per row.
        One doc per non-empty text field.
        """
        docs       = []
        skipped    = 0
        indexed_at = datetime.now().isoformat()

        for _, row in df.iterrows():
            procnr    = self._clean(row.get("PROCNR"))
            chunk_seq = 0

            for col in TEXT_COLUMNS:
                content = self._clean(row.get(col))
                if not content:
                    skipped += 1
                    continue

                doc_id = self._make_id(procnr, col, chunk_seq)
                docs.append({
                    "_index":  NERMAL_TEXT_INDEX,
                    "_id":     doc_id,
                    "_source": {
                        "id":             doc_id,
                        "PROCNR":         procnr,
                        "chunk_seq":      chunk_seq,
                        "content":        content,
                        "embedding":      None,
                        "source_columns": col,
                        "indexed_at":     indexed_at,
                    },
                })
                chunk_seq += 1

        print(f"  ✅ nermal_text docs prepared  : {len(docs):>8,}")
        print(f"  ⚠️  Skipped (empty text)        : {skipped:>8,}")
        return docs

    def prepare_tcfiles_docs(self, df: pd.DataFrame) -> list[dict]:
        """
        Parse the ATTACHMENTS JSON column and build one teamcenter_files
        document per attachment entry per row.
        """
        docs       = []
        skipped    = 0
        indexed_at = datetime.now().isoformat()

        for _, row in df.iterrows():
            procnr      = self._clean(row.get("PROCNR"))
            key         = self._clean(row.get("KEY"))
            attachments = self._clean(row.get("ATTACHMENTS"))

            if not attachments:
                skipped += 1
                continue

            # Parse the attachment list (stored as Python-literal string)
            try:
                att_list = ast.literal_eval(attachments)
            except Exception:
                skipped += 1
                continue

            for att in att_list:
                filename = self._clean(att.get("filename"))
                if not filename:
                    skipped += 1
                    continue

                orig_obid = key
                filepath  = self._clean(att.get("filepath"))
                filetype  = self._clean(att.get("filetype"))
                doctype   = self._clean(att.get("doctype"))
                doc_id    = self._make_id(orig_obid, filename)

                docs.append({
                    "_index":  TEAMCENTER_FILES_INDEX,
                    "_id":     doc_id,
                    "_source": {
                        "id":         doc_id,
                        "PROCNR":     procnr,
                        "filename":   filename,
                        "filepath":   filepath,
                        "filetype":   filetype,
                        "doctype":    doctype,
                        "chunk_seq":  0,
                        "content":    None,
                        "embedding":  None,
                        "indexed_at": indexed_at,
                        "orig_obid":  orig_obid,
                    },
                })

        print(f"  ✅ teamcenter_files docs prepared: {len(docs):>6,}")
        print(f"  ⚠️  Skipped (no attachment / parse error): {skipped:>3,}")
        return docs

    # ── Bulk index ────────────────────────────────────────────

    def bulk_index(self, docs: list[dict], batch_size: int = 200) -> tuple[int, int]:
        """
        Send docs to ES in batches via the _bulk API.
        Returns (success_count, failed_count).
        """
        total   = len(docs)
        success = 0
        failed  = 0

        print(f"  📤 Total docs : {total:,}")
        print(f"  {'─' * 50}")

        for i in range(0, total, batch_size):
            batch         = docs[i : i + batch_size]
            batch_num     = (i // batch_size) + 1
            total_batches = (total + batch_size - 1) // batch_size

            # Build NDJSON payload
            lines = []
            for doc in batch:
                lines.append(json.dumps({
                    "index": {"_index": doc["_index"], "_id": doc["_id"]}
                }))
                lines.append(json.dumps(doc["_source"], default=str))
            payload = "\n".join(lines) + "\n"

            res = self.es.post_ndjson("_bulk", payload)

            if res and res.status_code == 200:
                items    = res.json().get("items", [])
                ok_count = sum(
                    1 for item in items
                    if item.get("index", {}).get("status") in (200, 201)
                )
                success += ok_count
                failed  += len(batch) - ok_count

                if len(batch) - ok_count > 0:
                    errs = [
                        item["index"].get("error", {}).get("reason", "")
                        for item in items
                        if item.get("index", {}).get("status") not in (200, 201)
                    ][:2]
                    print(f"\n  ⚠️  Batch errors: {errs}")
            else:
                failed += len(batch)
                err     = res.text[:150] if res else "No response"
                print(f"\n  ❌ Batch {batch_num}: {err}")

            pct = min((i + batch_size), total) / total * 100
            bar = "█" * int(pct / 5) + "░" * (20 - int(pct / 5))
            print(
                f"  [{bar}] {pct:>5.1f}%  Batch {batch_num}/{total_batches}"
                f"  ✅ {success:,}  ❌ {failed}",
                end="\r",
            )

        print(f"\n  {'─' * 50}")
        print(f"  ✅ Success : {success:,}")
        print(f"  ❌ Failed  : {failed:,}")
        return success, failed

    # ── Refresh ───────────────────────────────────────────────

    def refresh(self, index_name: str):
        self.es.post(f"{index_name}/_refresh", {})


# ══════════════════════════════════════════════════════════════
# DATA LOADER
# ══════════════════════════════════════════════════════════════

class DataLoader:
    """Loads and lightly cleans the source CSV / XLS file."""

    def __init__(self, filepath: str):
        self.filepath = filepath

    def load(self) -> pd.DataFrame:
        """Read the file (CSV or Excel) and return a DataFrame."""
        if self.filepath.endswith((".xls", ".xlsx")):
            try:
                df = pd.read_excel(self.filepath, engine="xlrd")
            except Exception:
                df = pd.read_csv(self.filepath)
        else:
            df = pd.read_csv(self.filepath)

        df = df.where(pd.notna(df), None)
        print(f"  📂 Loaded  : {self.filepath}")
        print(f"  Rows       : {len(df):,}")
        print(f"  Columns    : {list(df.columns)}")
        return df


# ══════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description="Index sample_data into Elasticsearch")
    parser.add_argument("--file",     default="sample_data.xls", help="Input data file path")
    parser.add_argument("--recreate", action="store_true",        help="Drop and re-create indexes")
    parser.add_argument("--batch",    type=int, default=200,      help="Bulk batch size")
    args = parser.parse_args()

    # ── Connect ───────────────────────────────────────────────
    print("=" * 55)
    print("  Elasticsearch Indexer")
    print("=" * 55)

    client  = ElasticsearchClient(ES_HOST, ES_USERNAME, ES_PASSWORD)
    indexer = ESIndexer(client)

    if not client.ping():
        print("  ❌ Cannot reach Elasticsearch. Check host / credentials.")
        return

    print(f"  ✅ Connected : {ES_HOST}")

    # ── Load data ─────────────────────────────────────────────
    print("\n" + "=" * 55)
    print("  Loading Source Data")
    print("=" * 55)
    loader = DataLoader(args.file)
    df     = loader.load()

    # ── Setup indexes ─────────────────────────────────────────
    indexer.setup_indexes(recreate=args.recreate)

    # ── Prepare & index nermal_text ───────────────────────────
    print("\n" + "=" * 55)
    print("  Indexing → nermal_text")
    print("=" * 55)
    nermal_docs  = indexer.prepare_nermal_docs(df)
    n_ok, n_fail = indexer.bulk_index(nermal_docs, batch_size=args.batch)
    indexer.refresh(NERMAL_TEXT_INDEX)

    # ── Prepare & index teamcenter_files ──────────────────────
    print("\n" + "=" * 55)
    print("  Indexing → teamcenter_files")
    print("=" * 55)
    tc_docs      = indexer.prepare_tcfiles_docs(df)
    t_ok, t_fail = indexer.bulk_index(tc_docs, batch_size=args.batch)
    indexer.refresh(TEAMCENTER_FILES_INDEX)

    # ── Summary ───────────────────────────────────────────────
    print("\n" + "=" * 55)
    print("  ✅ INDEXING COMPLETE")
    print("=" * 55)
    print(f"  {'Metric':<40} {'Count':>8}")
    print(f"  {'─' * 50}")
    print(f"  {'nermal_text     indexed':<40} {n_ok:>8,}")
    print(f"  {'nermal_text     failed':<40} {n_fail:>8,}")
    print(f"  {'teamcenter_files indexed':<40} {t_ok:>8,}")
    print(f"  {'teamcenter_files failed':<40} {t_fail:>8,}")
    print("=" * 55)


if __name__ == "__main__":
    main()
