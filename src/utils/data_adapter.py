"""
Aurora Data Adapter
====================
Normalises any raw CSV into a format the Aurora extension pipeline
can process without schema errors, datetime misdetection, or memory issues.

Problems solved:
  1. Schema mismatch     — columns are renamed/selected to a stable set
  2. Datetime format     — any recognisable date string → ISO-8601
  3. Encoding / quoting  — pandas RFC-4180 parser handles embedded commas/quotes
  4. File size           — stratified sampling keeps output ≤ MAX_ROWS
  5. Missing target      — heuristic detection + explicit --target flag
  6. Delimiter           — sep=None auto-detection (comma, semicolon, tab, pipe)

Usage (CLI):
    python data_adapter.py <input.csv>
    python data_adapter.py <input.csv> --target is_fraud --max-rows 5000
    python data_adapter.py <input.csv> --out normalised.csv --no-sample

Python API:
    from data_adapter import adapt
    out_path = adapt("credit_card_frauds.csv", target_col="is_fraud")
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
import warnings
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

try:
    import pandas as pd
except ImportError:
    print(
        "ERROR: pandas is required.  Run: pip install pandas",
        file=sys.stderr,
    )
    sys.exit(1)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

MAX_ROWS: int = 5_000          # default sample ceiling sent to the pipeline
RANDOM_STATE: int = 42
MIN_ROWS_FOR_SAMPLING: int = 500   # files smaller than this are never sampled

# All datetime format strings tried in order (most → least specific).
_DT_FORMATS: List[str] = [
    "%Y-%m-%dT%H:%M:%S",   # ISO-8601 with T
    "%Y-%m-%d %H:%M:%S",   # ISO-8601 space-sep
    "%Y-%m-%d %H:%M",
    "%Y-%m-%d",
    "%d-%m-%Y %H:%M:%S",   # EU datetime  (15-05-2020 00:00:00)
    "%d-%m-%Y %H:%M",      # EU datetime  (15-05-2020 00:00)
    "%d-%m-%Y",            # EU date
    "%m/%d/%Y %H:%M:%S",   # US datetime
    "%m/%d/%Y %H:%M",
    "%m/%d/%Y",            # US date
    "%d/%m/%Y %H:%M:%S",
    "%d/%m/%Y %H:%M",
    "%d/%m/%Y",
    "%Y/%m/%d %H:%M:%S",
    "%Y/%m/%d",
    "%b %d %Y %H:%M:%S",   # Jan 15 2020 00:00:00
    "%d %b %Y",
    "%B %d, %Y",
]

# Column-name aliases → normalised name (lower-case key matching)
_TIMESTAMP_ALIASES: List[str] = [
    "date_time", "datetime", "timestamp", "date", "time",
    "trans_date_trans_time", "trans_date", "transaction_date",
    "order_date", "created_at", "updated_at", "event_time",
]

_TARGET_ALIASES: List[str] = [
    "is_fraud", "fraud", "label", "target", "class", "y",
    "outcome", "churn", "default", "is_default", "anomaly",
    "is_anomaly", "failure", "is_failure",
]


# ---------------------------------------------------------------------------
# Datetime helpers
# ---------------------------------------------------------------------------

def _try_parse_dt(value: str) -> Optional[datetime]:
    """Try every known format; return parsed datetime or None."""
    s = value.strip()
    for fmt in _DT_FORMATS:
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue
    # Last resort: pandas' flexible parser (covers locale variants)
    try:
        return pd.to_datetime(s, infer_datetime_format=True).to_pydatetime()
    except Exception:
        return None


def _is_datetime_column(series: pd.Series, probe: int = 30) -> bool:
    """Heuristic: returns True if ≥ 60 % of non-null samples parse as dates."""
    sample = series.dropna().astype(str).head(probe)
    if sample.empty:
        return False
    hits = sum(1 for v in sample if _try_parse_dt(v) is not None)
    return hits / len(sample) >= 0.6


def _convert_datetime_column(series: pd.Series) -> pd.Series:
    """Convert a string datetime series to ISO-8601 strings."""

    def _to_iso(v) -> str:
        if pd.isna(v):
            return ""
        dt = _try_parse_dt(str(v))
        if dt is None:
            return str(v)
        return dt.strftime("%Y-%m-%dT%H:%M:%S")

    return series.apply(_to_iso)


# ---------------------------------------------------------------------------
# Schema normalisation helpers
# ---------------------------------------------------------------------------

def _normalise_col_name(name: str) -> str:
    """Lower-case, strip whitespace, collapse internal spaces to underscore."""
    return re.sub(r"[^\w]", "_", name.strip().lower()).strip("_")


def _detect_timestamp_col(df: pd.DataFrame) -> Optional[str]:
    """Find the most likely timestamp column by alias or dtype heuristic."""
    norm_map: Dict[str, str] = {
        _normalise_col_name(c): c for c in df.columns
    }

    # Alias match first
    for alias in _TIMESTAMP_ALIASES:
        if alias in norm_map:
            return norm_map[alias]

    # Heuristic: first column that looks like a datetime
    for col in df.columns:
        if df[col].dtype == object and _is_datetime_column(df[col]):
            return col

    return None


def _detect_target_col(df: pd.DataFrame) -> Optional[str]:
    """Find the most likely target / label column."""
    norm_map: Dict[str, str] = {
        _normalise_col_name(c): c for c in df.columns
    }

    # Alias match first
    for alias in _TARGET_ALIASES:
        if alias in norm_map:
            return norm_map[alias]

    # Heuristic: last binary column (0/1 or two unique string values)
    for col in reversed(df.columns.tolist()):
        uniq = df[col].dropna().unique()
        if len(uniq) == 2:  # binary candidate
            return col

    return None


# ---------------------------------------------------------------------------
# Sampling
# ---------------------------------------------------------------------------

def _stratified_sample(
    df: pd.DataFrame,
    target_col: Optional[str],
    max_rows: int,
) -> pd.DataFrame:
    """
    Return a sample of at most `max_rows` rows.
    Uses stratified sampling when `target_col` is available; otherwise random.
    """
    if len(df) <= max_rows:
        return df

    if target_col and target_col in df.columns:
        try:
            # Proportional stratified sample
            sampled = (
                df.groupby(target_col, group_keys=False)
                .apply(lambda g: g.sample(
                    frac=max_rows / len(df),
                    random_state=RANDOM_STATE,
                ))
            )
            # Trim to exactly max_rows (groupby rounding may over-shoot)
            return sampled.head(max_rows).reset_index(drop=True)
        except Exception:
            pass  # fall through to random

    return df.sample(n=max_rows, random_state=RANDOM_STATE).reset_index(drop=True)


# ---------------------------------------------------------------------------
# Core adapter
# ---------------------------------------------------------------------------

def adapt(
    input_path: str,
    *,
    output_path: Optional[str] = None,
    target_col: Optional[str] = None,
    max_rows: int = MAX_ROWS,
    do_sample: bool = True,
    drop_cols: Optional[List[str]] = None,
    encoding: str = "utf-8",
    verbose: bool = True,
    clean_first: bool = True,
    outlier_multiplier: float = 3.0,
    drop_high_cardinality: bool = False,
) -> str:
    """
    Normalise `input_path` and write the result to `output_path`.

    Returns the path of the written file.

    Parameters
    ----------
    input_path            : Source CSV (any delimiter, any date format, any encoding).
    output_path           : Destination. Defaults to <stem>_adapted.csv beside the source.
    target_col            : Explicit target / label column name. Auto-detected if None.
    max_rows              : Sample ceiling (disabled when do_sample=False).
    do_sample             : Set False to keep every row.
    drop_cols             : Columns to remove before writing (e.g. high-cardinality IDs).
    encoding              : Source file encoding.
    verbose               : Print a JSON report to stdout when True.
    clean_first           : Run data_cleaner pipeline before adapting (default True).
    outlier_multiplier    : IQR fence multiplier passed to cleaner (default 3.0).
    drop_high_cardinality : Drop suspected ID/hash columns in cleaner (default False).
    """
    src = Path(input_path).resolve()
    if not src.exists():
        raise FileNotFoundError(src)

    # ------------------------------------------------------------------
    # 0. Optional cleaning pass (runs before everything else)
    # ------------------------------------------------------------------
    _clean_tmp: Optional[str] = None
    if clean_first:
        try:
            import importlib.util, sys as _sys
            _utils_dir = str(src.parent) if (src.parent / "data_cleaner.py").exists() \
                         else str(Path(__file__).parent)
            if _utils_dir not in _sys.path:
                _sys.path.insert(0, _utils_dir)
            from data_cleaner import clean as _clean_fn
            _clean_tmp = str(src.parent / f"{src.stem}_tmp_clean.csv")
            _clean_fn(
                str(src),
                out_path=_clean_tmp,
                outlier_multiplier=outlier_multiplier,
                drop_high_cardinality=drop_high_cardinality,
                encoding=encoding,
                verbose=verbose,
            )
            src = Path(_clean_tmp)
        except Exception as _e:
            # Cleaner unavailable or failed — continue without it
            _clean_tmp = None

    report: Dict = {
        "source": str(src),
        "issues_found": [],
        "fixes_applied": [],
        "clean_pass": "applied" if _clean_tmp else "skipped",
    }

    # ------------------------------------------------------------------
    # 1. Load — auto-detect delimiter (issue #6)
    # ------------------------------------------------------------------
    try:
        df = pd.read_csv(
            str(src),
            sep=None,            # auto-detect: comma, semicolon, tab, pipe
            engine="python",     # required for sep=None
            encoding=encoding,
            encoding_errors="replace",
            on_bad_lines="warn",
            low_memory=False,
        )
    except UnicodeDecodeError:
        # Try latin-1 fallback
        report["issues_found"].append("encoding: UTF-8 failed, retried with latin-1")
        report["fixes_applied"].append("encoding: latin-1 fallback")
        df = pd.read_csv(
            str(src),
            sep=None,
            engine="python",
            encoding="latin-1",
            on_bad_lines="warn",
            low_memory=False,
        )

    original_rows, original_cols = df.shape
    report["original_shape"] = {"rows": original_rows, "cols": original_cols}

    # ------------------------------------------------------------------
    # 2. Drop explicitly unwanted columns
    # ------------------------------------------------------------------
    if drop_cols:
        to_drop = [c for c in drop_cols if c in df.columns]
        df.drop(columns=to_drop, inplace=True)
        if to_drop:
            report["fixes_applied"].append(f"dropped_cols: {to_drop}")

    # ------------------------------------------------------------------
    # 3. Detect & convert datetime columns (issue #2)
    # ------------------------------------------------------------------
    ts_col = _detect_timestamp_col(df)
    if ts_col:
        sample_val = df[ts_col].dropna().astype(str).iloc[0] if not df[ts_col].dropna().empty else ""
        if sample_val and not re.match(r"\d{4}-\d{2}-\d{2}T", sample_val):
            report["issues_found"].append(
                f"datetime: column '{ts_col}' has non-ISO format (sample: '{sample_val}')"
            )
            df[ts_col] = _convert_datetime_column(df[ts_col])
            report["fixes_applied"].append(
                f"datetime: '{ts_col}' converted to ISO-8601"
            )

    # ------------------------------------------------------------------
    # 4. Detect target column (issue #5)
    # ------------------------------------------------------------------
    resolved_target = target_col or _detect_target_col(df)
    report["target_column"] = resolved_target

    if resolved_target is None:
        report["issues_found"].append(
            "target: no label/target column detected — dataset treated as unsupervised"
        )
    elif resolved_target not in df.columns:
        report["issues_found"].append(
            f"target: specified column '{resolved_target}' not found — ignored"
        )
        resolved_target = None
    else:
        report["fixes_applied"].append(
            f"target: '{resolved_target}' identified as label column"
        )

    # ------------------------------------------------------------------
    # 5. Sample for large files (issue #4)
    # ------------------------------------------------------------------
    if do_sample and len(df) > MIN_ROWS_FOR_SAMPLING:
        if len(df) > max_rows:
            report["issues_found"].append(
                f"size: {len(df):,} rows exceeds limit ({max_rows:,})"
            )
            df = _stratified_sample(df, resolved_target, max_rows)
            report["fixes_applied"].append(
                f"size: stratified sample → {len(df):,} rows"
            )

    # ------------------------------------------------------------------
    # 6. Write output
    # ------------------------------------------------------------------
    if output_path is None:
        out = src.parent / f"{src.stem}_adapted.csv"
    else:
        out = Path(output_path).resolve()

    df.to_csv(str(out), index=False, encoding="utf-8")

    # Cleanup intermediate clean file if it was created
    import os as _os
    if _clean_tmp and _os.path.exists(_clean_tmp) and _clean_tmp != str(out):
        _os.remove(_clean_tmp)

    report["output_path"] = str(out)
    report["output_shape"] = {"rows": len(df), "cols": len(df.columns)}
    report["columns"] = list(df.columns)

    if verbose:
        print(json.dumps(report, indent=2, ensure_ascii=False))

    return str(out)


# ---------------------------------------------------------------------------
# Batch adapter: normalise multiple files at once
# ---------------------------------------------------------------------------

def adapt_batch(
    paths: List[str],
    output_dir: Optional[str] = None,
    **kwargs,
) -> Dict[str, str]:
    """
    Normalise multiple CSV files.

    Returns a dict mapping input path → output path.
    """
    results: Dict[str, str] = {}
    for p in paths:
        try:
            out_path: Optional[str] = None
            if output_dir:
                stem = Path(p).stem
                out_path = str(Path(output_dir) / f"{stem}_adapted.csv")
            results[p] = adapt(p, output_path=out_path, **kwargs)
        except Exception as exc:
            results[p] = f"ERROR: {exc}"
    return results


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="data_adapter",
        description="Normalise a raw CSV for Aurora extension compatibility.",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    p.add_argument("input", help="Path to the source CSV file.")
    p.add_argument("--out", metavar="PATH", help="Output file path (default: <stem>_adapted.csv).")
    p.add_argument("--target", metavar="COL", help="Target / label column name.")
    p.add_argument("--max-rows", type=int, default=MAX_ROWS, metavar="N",
                   help=f"Maximum rows in output (default: {MAX_ROWS}).")
    p.add_argument("--no-sample", action="store_true",
                   help="Disable sampling — keep all rows.")
    p.add_argument("--drop", nargs="*", metavar="COL",
                   help="Column names to remove from output.")
    p.add_argument("--encoding", default="utf-8",
                   help="Source encoding (default: utf-8).")
    p.add_argument("--quiet", action="store_true",
                   help="Suppress the JSON report.")
    return p


def _main(argv: Optional[List[str]] = None) -> int:
    args = _build_parser().parse_args(argv)
    try:
        adapt(
            args.input,
            output_path=args.out,
            target_col=args.target,
            max_rows=args.max_rows,
            do_sample=not args.no_sample,
            drop_cols=args.drop or [],
            encoding=args.encoding,
            verbose=not args.quiet,
        )
        return 0
    except FileNotFoundError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    except Exception as exc:
        print(f"UNEXPECTED ERROR: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(_main())
