"""
es_retriever.py
─────────────────────────────────────────────────────────────
Retrieves documents from Elasticsearch indexes:
  • nermal_text         – text chunks
  • teamcenter_files    – attachment metadata

Usage examples
--------------
    # All docs
    python es_retriever.py --query all

    # By PROCNR
    python es_retriever.py --query procnr --procnr "NER_2020/21_01192_SIN"

    # Full-text search
    python es_retriever.py --query text --search "cleaning process"

    # By source column
    python es_retriever.py --query column --column MOTIVATION

    # Files by filetype
    python es_retriever.py --query filetype --filetype xlsx

    # Aggregation stats
    python es_retriever.py --query agg

    # Index counts
    python es_retriever.py --query count
"""

import argparse
import urllib3
import warnings
import requests
import pandas as pd

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


# ══════════════════════════════════════════════════════════════
# ELASTICSEARCH CLIENT  (same thin wrapper as in es_indexer.py)
# ══════════════════════════════════════════════════════════════

class ElasticsearchClient:
    """Thin wrapper around requests for Elasticsearch REST calls."""

    JSON_HEADERS = {"Content-Type": "application/json", "Accept": "application/json"}

    def __init__(self, host: str, username: str, password: str, verify_ssl: bool = False):
        self.host   = host.rstrip("/")
        self.auth   = (username, password)
        self.verify = verify_ssl

    def _url(self, path: str) -> str:
        return f"{self.host}/{path.lstrip('/')}"

    def get(self, path: str):
        try:
            return requests.get(
                self._url(path),
                auth=self.auth, headers=self.JSON_HEADERS,
                verify=self.verify, timeout=10,
            )
        except Exception as exc:
            print(f"  ❌ GET {path}: {exc}")
            return None

    def post(self, path: str, body: dict, timeout: int = 30):
        try:
            return requests.post(
                self._url(path),
                auth=self.auth, headers=self.JSON_HEADERS,
                json=body, verify=self.verify, timeout=timeout,
            )
        except Exception as exc:
            print(f"  ❌ POST {path}: {exc}")
            return None

    def ping(self) -> bool:
        res = self.get("/")
        return res is not None and res.status_code == 200


# ══════════════════════════════════════════════════════════════
# RETRIEVER
# ══════════════════════════════════════════════════════════════

class ESRetriever:
    """
    All retrieval / search operations against nermal_text and
    teamcenter_files indexes.
    """

    def __init__(self, client: ElasticsearchClient):
        self.es = client

    # ── Private helpers ───────────────────────────────────────

    def _search(self, index: str, query: dict, size: int = 100) -> pd.DataFrame:
        """
        Run an ES _search query and return a DataFrame of _source docs.
        Returns an empty DataFrame if the call fails or no hits.
        """
        res = self.es.post(f"{index}/_search?size={size}", query)

        if not res or res.status_code != 200:
            code = res.status_code if res else "N/A"
            err  = (res.json().get("error", {}).get("reason", "") if res else "")
            print(f"  ❌ Search [{code}]: {err}")
            return pd.DataFrame()

        hits  = res.json()["hits"]["hits"]
        total = res.json()["hits"]["total"]["value"]

        if not hits:
            print("  ⚠️  No results found")
            return pd.DataFrame()

        df = pd.DataFrame([h["_source"] for h in hits])
        print(f"  ✅ Total matched: {total:,}  |  Fetched: {len(hits)}")
        return df

    def _agg(self, index: str, agg_body: dict) -> dict:
        """Run an aggregation query (size=0) and return raw aggregations."""
        body = {"size": 0, "aggs": agg_body}
        res  = self.es.post(f"{index}/_search", body)

        if res and res.status_code == 200:
            return res.json().get("aggregations", {})

        code = res.status_code if res else "N/A"
        print(f"  ❌ Aggregation [{code}]")
        return {}

    # ── Count ─────────────────────────────────────────────────

    def get_count(self, index: str) -> int:
        """Return the total document count for an index."""
        res = self.es.get(f"{index}/_count")
        if res and res.status_code == 200:
            return res.json().get("count", 0)
        return 0

    def print_counts(self):
        """Print doc counts for both indexes."""
        print("\n" + "=" * 55)
        print("  Index Document Counts")
        print("=" * 55)
        for idx in (NERMAL_TEXT_INDEX, TEAMCENTER_FILES_INDEX):
            count = self.get_count(idx)
            print(f"  {idx:<40} : {count:>8,}")

    # ── All documents ─────────────────────────────────────────

    def get_all_nermal(self, size: int = 100) -> pd.DataFrame:
        """Fetch all nermal_text docs, ordered by PROCNR then chunk_seq."""
        print(f"\n  🔍 All nermal_text docs (limit={size})")
        return self._search(
            NERMAL_TEXT_INDEX,
            {
                "query": {"match_all": {}},
                "sort":  [
                    {"PROCNR":    {"order": "asc"}},
                    {"chunk_seq": {"order": "asc"}},
                ],
            },
            size=size,
        )

    def get_all_tcfiles(self, size: int = 100) -> pd.DataFrame:
        """Fetch all teamcenter_files docs, ordered by PROCNR."""
        print(f"\n  🔍 All teamcenter_files docs (limit={size})")
        return self._search(
            TEAMCENTER_FILES_INDEX,
            {"query": {"match_all": {}}, "sort": [{"PROCNR": {"order": "asc"}}]},
            size=size,
        )

    # ── By PROCNR ─────────────────────────────────────────────

    def get_nermal_by_procnr(self, procnr: str, size: int = 50) -> pd.DataFrame:
        """
        Return all nermal_text chunks for a given PROCNR,
        sorted by chunk_seq.
        """
        print(f"\n  🔍 nermal_text → PROCNR = {procnr}")
        return self._search(
            NERMAL_TEXT_INDEX,
            {
                "query": {"term": {"PROCNR": procnr}},
                "sort":  [{"chunk_seq": {"order": "asc"}}],
            },
            size=size,
        )

    def get_tcfiles_by_procnr(self, procnr: str, size: int = 50) -> pd.DataFrame:
        """Return all teamcenter_files docs for a given PROCNR."""
        print(f"\n  🔍 teamcenter_files → PROCNR = {procnr}")
        return self._search(
            TEAMCENTER_FILES_INDEX,
            {"query": {"term": {"PROCNR": procnr}}},
            size=size,
        )

    def get_by_procnr(self, procnr: str) -> tuple[pd.DataFrame, pd.DataFrame]:
        """
        Convenience: return (nermal_df, tcfiles_df) for a PROCNR.
        """
        df_nermal = self.get_nermal_by_procnr(procnr)
        df_tc     = self.get_tcfiles_by_procnr(procnr)
        return df_nermal, df_tc

    # ── Full-text search ──────────────────────────────────────

    def search_content(self, text: str, size: int = 10) -> pd.DataFrame:
        """
        Full-text search across the content field in nermal_text.
        Uses AND operator so every word must appear.
        """
        print(f"\n  🔍 Full-text search → \"{text}\"")
        return self._search(
            NERMAL_TEXT_INDEX,
            {
                "query": {
                    "match": {
                        "content": {"query": text, "operator": "and"}
                    }
                },
                "highlight": {"fields": {"content": {}}},
            },
            size=size,
        )

    def search_filename(self, text: str, size: int = 20) -> pd.DataFrame:
        """Full-text search across the filename.text sub-field."""
        print(f"\n  🔍 Filename search → \"{text}\"")
        return self._search(
            TEAMCENTER_FILES_INDEX,
            {
                "query": {
                    "match": {"filename.text": {"query": text, "operator": "or"}}
                }
            },
            size=size,
        )

    # ── By source column ──────────────────────────────────────

    def get_by_source_column(self, column: str, size: int = 50) -> pd.DataFrame:
        """
        Return nermal_text docs that originate from a specific
        source column (TITLE, MOTIVATION, CURRENT_PROCEDURE, etc.).
        """
        print(f"\n  🔍 nermal_text → source_columns = {column}")
        return self._search(
            NERMAL_TEXT_INDEX,
            {"query": {"term": {"source_columns": column}}},
            size=size,
        )

    # ── By filetype ───────────────────────────────────────────

    def get_files_by_type(self, filetype: str, size: int = 50) -> pd.DataFrame:
        """Return teamcenter_files docs of a given file extension."""
        print(f"\n  🔍 teamcenter_files → filetype = {filetype}")
        return self._search(
            TEAMCENTER_FILES_INDEX,
            {"query": {"term": {"filetype": filetype}}},
            size=size,
        )

    # ── By doctype ────────────────────────────────────────────

    def get_files_by_doctype(self, doctype: str, size: int = 50) -> pd.DataFrame:
        """Return teamcenter_files docs of a given document type."""
        print(f"\n  🔍 teamcenter_files → doctype = {doctype}")
        return self._search(
            TEAMCENTER_FILES_INDEX,
            {"query": {"term": {"doctype": doctype}}},
            size=size,
        )

    # ── Aggregations ──────────────────────────────────────────

    def agg_nermal_stats(self) -> None:
        """Print aggregation: docs per source_column and per PROCNR."""
        print("\n" + "=" * 55)
        print("  Aggregation: nermal_text Stats")
        print("=" * 55)

        aggs = self._agg(
            NERMAL_TEXT_INDEX,
            {
                "by_source": {"terms": {"field": "source_columns", "size": 20}},
                "by_procnr": {"terms": {"field": "PROCNR",         "size": 20}},
            },
        )

        print("\n  Docs per Source Column:")
        print(f"  {'─' * 35}")
        for b in aggs.get("by_source", {}).get("buckets", []):
            print(f"  {b['key']:<30} : {b['doc_count']:>6}")

        print("\n  Docs per PROCNR:")
        print(f"  {'─' * 35}")
        for b in aggs.get("by_procnr", {}).get("buckets", []):
            print(f"  {b['key']:<30} : {b['doc_count']:>6}")

    def agg_tcfiles_stats(self) -> None:
        """Print aggregation: docs per filetype and per doctype."""
        print("\n" + "=" * 55)
        print("  Aggregation: teamcenter_files Stats")
        print("=" * 55)

        aggs = self._agg(
            TEAMCENTER_FILES_INDEX,
            {
                "by_filetype": {"terms": {"field": "filetype", "size": 20}},
                "by_doctype":  {"terms": {"field": "doctype",  "size": 20}},
            },
        )

        print("\n  Docs per Filetype:")
        print(f"  {'─' * 35}")
        for b in aggs.get("by_filetype", {}).get("buckets", []):
            print(f"  {b['key']:<30} : {b['doc_count']:>6}")

        print("\n  Docs per DocType:")
        print(f"  {'─' * 35}")
        for b in aggs.get("by_doctype", {}).get("buckets", []):
            print(f"  {b['key']:<30} : {b['doc_count']:>6}")


# ══════════════════════════════════════════════════════════════
# DISPLAY HELPERS
# ══════════════════════════════════════════════════════════════

def show_nermal(df: pd.DataFrame, max_content: int = 60):
    """Pretty-print a nermal_text DataFrame."""
    if df.empty:
        return
    cols    = [c for c in ("PROCNR", "chunk_seq", "source_columns", "content") if c in df.columns]
    display = df[cols].copy()
    if "content" in display.columns:
        display["content"] = display["content"].str[:max_content]
    print(display.to_string(index=False))


def show_tcfiles(df: pd.DataFrame):
    """Pretty-print a teamcenter_files DataFrame."""
    if df.empty:
        return
    cols = [c for c in ("PROCNR", "filename", "filetype", "doctype", "filepath") if c in df.columns]
    print(df[cols].to_string(index=False))


# ══════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description="Retrieve data from Elasticsearch")
    parser.add_argument(
        "--query",
        choices=["all", "procnr", "text", "column", "filetype", "doctype", "agg", "count"],
        default="all",
        help="Query type to run",
    )
    parser.add_argument("--procnr",   default="NER_2020/21_01192_SIN", help="PROCNR to filter by")
    parser.add_argument("--search",   default="cleaning process",       help="Text for full-text search")
    parser.add_argument("--column",   default="MOTIVATION",             help="source_columns value")
    parser.add_argument("--filetype", default="xlsx",                   help="File extension to filter")
    parser.add_argument("--doctype",  default="UNKNOWN",                help="Document type to filter")
    parser.add_argument("--size",     type=int, default=20,             help="Max results to return")
    args = parser.parse_args()

    # ── Connect ───────────────────────────────────────────────
    print("=" * 55)
    print("  Elasticsearch Retriever")
    print("=" * 55)

    client    = ElasticsearchClient(ES_HOST, ES_USERNAME, ES_PASSWORD)
    retriever = ESRetriever(client)

    if not client.ping():
        print("  ❌ Cannot reach Elasticsearch. Check host / credentials.")
        return

    print(f"  ✅ Connected : {ES_HOST}")

    # ── Dispatch ──────────────────────────────────────────────
    q = args.query

    if q == "count":
        retriever.print_counts()

    elif q == "all":
        print("\n" + "=" * 55)
        print("  All nermal_text")
        print("=" * 55)
        show_nermal(retriever.get_all_nermal(size=args.size))

        print("\n" + "=" * 55)
        print("  All teamcenter_files")
        print("=" * 55)
        show_tcfiles(retriever.get_all_tcfiles(size=args.size))

    elif q == "procnr":
        df_n, df_tc = retriever.get_by_procnr(args.procnr)

        print("\n" + "=" * 55)
        print(f"  nermal_text → {args.procnr}")
        print("=" * 55)
        show_nermal(df_n)

        print("\n" + "=" * 55)
        print(f"  teamcenter_files → {args.procnr}")
        print("=" * 55)
        show_tcfiles(df_tc)

    elif q == "text":
        print("\n" + "=" * 55)
        print(f"  Full-text search → \"{args.search}\"")
        print("=" * 55)
        show_nermal(retriever.search_content(args.search, size=args.size))

    elif q == "column":
        print("\n" + "=" * 55)
        print(f"  By source_columns → {args.column}")
        print("=" * 55)
        show_nermal(retriever.get_by_source_column(args.column, size=args.size))

    elif q == "filetype":
        print("\n" + "=" * 55)
        print(f"  teamcenter_files → filetype = {args.filetype}")
        print("=" * 55)
        show_tcfiles(retriever.get_files_by_type(args.filetype, size=args.size))

    elif q == "doctype":
        print("\n" + "=" * 55)
        print(f"  teamcenter_files → doctype = {args.doctype}")
        print("=" * 55)
        show_tcfiles(retriever.get_files_by_doctype(args.doctype, size=args.size))

    elif q == "agg":
        retriever.agg_nermal_stats()
        retriever.agg_tcfiles_stats()


if __name__ == "__main__":
    main()
