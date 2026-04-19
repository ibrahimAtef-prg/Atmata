"""
Aurora Data Cleaner
===================
الخطوة التي تسبق baseline.py مباشرةً.

لماذا لا نستخدم StandardScaler / MinMaxScaler هنا؟
----------------------------------------------------
generator.py يبني synthetic data باستخدام quantile-based inverse CDF
محفوظة من baseline.py (q01, q25, q50, q75, q99, min, max).
لو طبّقنا scaling قبل baseline، سيحفظ baseline الـ quantiles على scale مختلف
والـ generator سيُنتج بيانات على نفس الـ scale المُعدَّل بدون أي
de-normalization — النتيجة: synthetic data خارج النطاق الحقيقي تماماً.

ما يُطبَّق هنا (آمن 100% مع pipeline):
  1. Structural:  إزالة duplicates، إصلاح column names
  2. Nulls:       median (numeric) / mode (categorical) imputation
  3. Outliers:    IQR capping فقط — يحمي الـ quantiles من القيم المتطرفة
  4. Strings:     توحيد whitespace والـ case
  5. Datetime:    تحويل لـ ISO-8601 (يتفاعل مع data_adapter)
  6. High-card:   تحذير عن أعمدة ID/hash عالية الـ cardinality

ما لا يُطبَّق هنا (يكسر pipeline):
  ✗  StandardScaler / z-score normalization
  ✗  MinMaxScaler
  ✗  RobustScaler
  ✗  Log transform على الأعمدة المستخدمة في الـ baseline مباشرةً
     (مسموح فقط كـ optional auxiliary column بعلم المستخدم)

Usage (CLI):
    python data_cleaner.py input.csv
    python data_cleaner.py input.csv --out clean.csv --outlier-multiplier 3.0
    python data_cleaner.py input.csv --drop-high-cardinality --report

Python API:
    from data_cleaner import clean
    result = clean("raw.csv", out_path="clean.csv")
    print(result.report)
"""

from __future__ import annotations

import argparse
import json
import math
import os
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

try:
    import pandas as pd
    import numpy as np
except ImportError:
    print("ERROR: pandas and numpy are required.  pip install pandas numpy", file=sys.stderr)
    sys.exit(1)


# ---------------------------------------------------------------------------
# Result dataclass
# ---------------------------------------------------------------------------

@dataclass
class CleanResult:
    output_path: str
    original_shape: Tuple[int, int]
    final_shape: Tuple[int, int]
    report: Dict[str, Any] = field(default_factory=dict)

    def print_report(self) -> None:
        print(json.dumps(self.report, indent=2, ensure_ascii=False))


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

# IQR multiplier for outlier capping (1.5 = classic Tukey, 3.0 = only extremes)
DEFAULT_IQR_MULTIPLIER: float = 3.0

# Cardinality ratio above which a column is flagged as likely-ID
HIGH_CARD_RATIO: float = 0.95

# Min non-null values to attempt any numeric stats
MIN_VALID: int = 5

# Datetime format list (same as data_adapter.py)
_DT_FORMATS: List[str] = [
    "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d",
    "%d-%m-%Y %H:%M:%S", "%d-%m-%Y %H:%M", "%d-%m-%Y",
    "%m/%d/%Y %H:%M:%S", "%m/%d/%Y %H:%M", "%m/%d/%Y",
    "%d/%m/%Y %H:%M:%S", "%d/%m/%Y %H:%M", "%d/%m/%Y",
    "%Y/%m/%d %H:%M:%S", "%Y/%m/%d",
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _try_parse_dt(s: str):
    """Return parsed datetime string → ISO-8601 or original string."""
    from datetime import datetime
    v = s.strip()
    for fmt in _DT_FORMATS:
        try:
            return datetime.strptime(v, fmt).strftime("%Y-%m-%dT%H:%M:%S")
        except ValueError:
            pass
    try:
        return pd.to_datetime(v).strftime("%Y-%m-%dT%H:%M:%S")
    except Exception:
        return v


def _is_datetime_col(series: pd.Series, probe: int = 30) -> bool:
    sample = series.dropna().astype(str).head(probe)
    if len(sample) < 3:
        return False
    hits = 0
    for v in sample:
        stripped = v.strip()
        if re.search(r"\d{4}-\d{2}-\d{2}", stripped):
            hits += 1
        elif re.search(r"\d{2}[-/]\d{2}[-/]\d{4}", stripped):
            hits += 1
    return hits / len(sample) >= 0.6


def _clean_column_name(name: str) -> str:
    """Lowercase, strip, collapse non-alphanumeric to underscore."""
    n = re.sub(r"[^\w]", "_", str(name).strip().lower())
    return re.sub(r"_+", "_", n).strip("_")


def _iqr_bounds(series: pd.Series, multiplier: float) -> Tuple[float, float]:
    """Return (lower, upper) IQR-based capping bounds."""
    q25 = series.quantile(0.25)
    q75 = series.quantile(0.75)
    iqr = q75 - q25
    if iqr == 0:
        return (-math.inf, math.inf)
    return (q25 - multiplier * iqr, q75 + multiplier * iqr)


# ---------------------------------------------------------------------------
# Individual cleaning steps
# ---------------------------------------------------------------------------

def _step_rename_columns(df: pd.DataFrame, log: List[str]) -> pd.DataFrame:
    """Normalise column names: lowercase + underscores."""
    old_names = list(df.columns)
    new_names = [_clean_column_name(c) for c in old_names]

    # Handle duplicate names after normalisation
    seen: Dict[str, int] = {}
    deduped: List[str] = []
    for n in new_names:
        if n in seen:
            seen[n] += 1
            deduped.append(f"{n}_{seen[n]}")
        else:
            seen[n] = 0
            deduped.append(n)

    renamed = {old: new for old, new in zip(old_names, deduped) if old != new}
    if renamed:
        df = df.rename(columns=renamed)
        log.append(f"columns_renamed: {renamed}")
    return df


def _step_remove_duplicates(df: pd.DataFrame, log: List[str]) -> pd.DataFrame:
    """Remove exact duplicate rows."""
    n_before = len(df)
    df = df.drop_duplicates()
    removed = n_before - len(df)
    if removed:
        log.append(f"duplicates_removed: {removed} rows")
    return df


def _step_clean_strings(df: pd.DataFrame, log: List[str]) -> pd.DataFrame:
    """
    For object columns that are NOT datetime:
    - Strip leading/trailing whitespace
    - Collapse internal multiple spaces to single
    - Unify empty-string/whitespace-only → NaN
    """
    cleaned: List[str] = []
    for col in df.select_dtypes(include="object").columns:
        if _is_datetime_col(df[col]):
            continue
        before_nulls = df[col].isna().sum()
        df[col] = (
            df[col]
            .astype(str)
            .str.strip()
            .str.replace(r"\s{2,}", " ", regex=True)
            .replace({"": np.nan, "nan": np.nan, "None": np.nan, "NULL": np.nan, "NA": np.nan})
        )
        after_nulls = df[col].isna().sum()
        if after_nulls > before_nulls:
            cleaned.append(f"{col}: {after_nulls - before_nulls} empty→NaN")
    if cleaned:
        log.append(f"strings_cleaned: {cleaned}")
    return df


def _step_impute_nulls(df: pd.DataFrame, log: List[str]) -> pd.DataFrame:
    """
    Numeric:     median imputation (robust to outliers)
    Categorical: mode  imputation (most frequent value)
    Datetime:    leave as NaN (fill would fabricate time points)
    """
    imputed: Dict[str, str] = {}

    # Numeric
    for col in df.select_dtypes(include=[np.number]).columns:
        null_count = df[col].isna().sum()
        if null_count == 0:
            continue
        valid = df[col].dropna()
        if len(valid) < MIN_VALID:
            continue
        fill_val = valid.median()
        df[col] = df[col].fillna(fill_val)
        imputed[col] = f"median={fill_val:.4g} ({null_count} nulls)"

    # Categorical / object (non-datetime)
    for col in df.select_dtypes(include="object").columns:
        if _is_datetime_col(df[col]):
            continue
        null_count = df[col].isna().sum()
        if null_count == 0:
            continue
        vc = df[col].value_counts(dropna=True)
        if vc.empty:
            continue
        fill_val = str(vc.index[0])
        df[col] = df[col].fillna(fill_val)
        imputed[col] = f"mode='{fill_val}' ({null_count} nulls)"

    if imputed:
        log.append(f"null_imputation: {imputed}")
    return df


def _step_cap_outliers(
    df: pd.DataFrame,
    log: List[str],
    multiplier: float,
) -> pd.DataFrame:
    """
    IQR capping on numeric columns.

    WHY capping and NOT removing:
    - Removing distorts class distributions (critical for label-correlated outliers)
    - Capping to IQR bounds preserves the quantile structure that baseline.py
      and generator.py rely on for accurate synthetic sampling

    WHY NOT StandardScaler / MinMax here:
    - generator._sample_numeric_col uses stored quantile points from the
      original scale to reconstruct the distribution via piecewise-linear CDF.
    - Applying any linear/non-linear scale transform changes those quantile
      points permanently and the generator will output data in the wrong range
      with no mechanism to reverse it.
    """
    capped: Dict[str, Dict] = {}
    for col in df.select_dtypes(include=[np.number]).columns:
        valid = df[col].dropna()
        if len(valid) < MIN_VALID:
            continue
        lo, hi = _iqr_bounds(valid, multiplier)
        if lo == -math.inf and hi == math.inf:
            continue
        mask_lo = df[col] < lo
        mask_hi = df[col] > hi
        n_lo = int(mask_lo.sum())
        n_hi = int(mask_hi.sum())
        if n_lo + n_hi == 0:
            continue
        if lo != -math.inf:
            df.loc[mask_lo, col] = lo
        if hi != math.inf:
            df.loc[mask_hi, col] = hi
        capped[col] = {
            "capped_low": n_lo,
            "capped_high": n_hi,
            "bounds": [round(lo, 4), round(hi, 4)],
        }

    if capped:
        log.append(f"outlier_capping_iqr_{multiplier}x: {capped}")
    return df


def _step_convert_datetimes(df: pd.DataFrame, log: List[str]) -> pd.DataFrame:
    """Convert detected datetime columns to ISO-8601 strings."""
    converted: List[str] = []
    for col in df.select_dtypes(include="object").columns:
        if not _is_datetime_col(df[col]):
            continue
        sample = df[col].dropna().astype(str).iloc[0] if not df[col].dropna().empty else ""
        if re.match(r"\d{4}-\d{2}-\d{2}T", sample):
            continue  # already ISO
        df[col] = df[col].apply(
            lambda v: _try_parse_dt(str(v)) if pd.notna(v) else v
        )
        converted.append(col)
    if converted:
        log.append(f"datetime_iso_converted: {converted}")
    return df


def _step_flag_high_cardinality(
    df: pd.DataFrame,
    log: List[str],
    drop: bool,
) -> Tuple[pd.DataFrame, List[str]]:
    """
    Flag (and optionally drop) object columns that look like ID/hash columns.
    These pollute categorical statistics in baseline.py.
    """
    flagged: List[str] = []
    n_rows = len(df)
    for col in df.select_dtypes(include="object").columns:
        if _is_datetime_col(df[col]):
            continue
        ratio = df[col].nunique(dropna=True) / max(n_rows, 1)
        if ratio >= HIGH_CARD_RATIO:
            flagged.append(col)

    if flagged:
        log.append(f"high_cardinality_cols: {flagged} (ratio ≥ {HIGH_CARD_RATIO})")
        if drop:
            df = df.drop(columns=flagged)
            log.append(f"high_cardinality_dropped: {flagged}")

    return df, flagged


def _step_coerce_numeric_strings(df: pd.DataFrame, log: List[str]) -> pd.DataFrame:
    """
    Convert object columns whose values are all numeric strings to float.
    Example: ['1.0', '2.5', '3.0'] → float64 column.
    """
    coerced: List[str] = []
    for col in df.select_dtypes(include="object").columns:
        if _is_datetime_col(df[col]):
            continue
        sample = df[col].dropna().head(200)
        if sample.empty:
            continue
        converted = pd.to_numeric(sample, errors="coerce")
        if converted.notna().sum() / len(sample) >= 0.95:
            df[col] = pd.to_numeric(df[col], errors="coerce")
            coerced.append(col)
    if coerced:
        log.append(f"numeric_string_coerced: {coerced}")
    return df


# ---------------------------------------------------------------------------
# Main clean() function
# ---------------------------------------------------------------------------

def clean(
    input_path: str,
    *,
    out_path: Optional[str] = None,
    outlier_multiplier: float = DEFAULT_IQR_MULTIPLIER,
    drop_high_cardinality: bool = False,
    impute_nulls: bool = True,
    cap_outliers: bool = True,
    fix_datetimes: bool = True,
    encoding: str = "utf-8",
    verbose: bool = True,
) -> CleanResult:
    """
    Clean and structurally normalise a CSV for Aurora pipeline ingestion.

    Returns a CleanResult with output path + full audit report.

    Parameters
    ----------
    input_path            : Source CSV file.
    out_path              : Destination (default: <stem>_clean.csv).
    outlier_multiplier    : IQR fence multiplier (default 3.0 = only extremes).
    drop_high_cardinality : Drop suspected ID/hash columns (default False = warn only).
    impute_nulls          : Fill nulls with median/mode (default True).
    cap_outliers          : Cap extreme values with IQR bounds (default True).
    fix_datetimes         : Convert date strings to ISO-8601 (default True).
    encoding              : Source file encoding.
    verbose               : Print JSON report to stdout.
    """
    src = Path(input_path).resolve()
    if not src.exists():
        raise FileNotFoundError(src)

    log: List[str] = []
    warnings_list: List[str] = []

    # ------------------------------------------------------------------ Load
    try:
        df = pd.read_csv(
            str(src), sep=None, engine="python",
            encoding=encoding, encoding_errors="replace",
            on_bad_lines="warn", low_memory=False,
        )
    except UnicodeDecodeError:
        warnings_list.append("encoding: UTF-8 failed, retried with latin-1")
        df = pd.read_csv(
            str(src), sep=None, engine="python",
            encoding="latin-1", on_bad_lines="warn", low_memory=False,
        )

    original_shape = df.shape

    # ------------------------------------------------------------------ Steps
    df = _step_rename_columns(df, log)
    df = _step_remove_duplicates(df, log)
    df = _step_clean_strings(df, log)
    df = _step_coerce_numeric_strings(df, log)

    if fix_datetimes:
        df = _step_convert_datetimes(df, log)

    df, high_card_cols = _step_flag_high_cardinality(df, log, drop=drop_high_cardinality)

    if impute_nulls:
        df = _step_impute_nulls(df, log)

    if cap_outliers:
        df = _step_cap_outliers(df, log, multiplier=outlier_multiplier)

    # ------------------------------------------------------------ Write output
    if out_path is None:
        out = src.parent / f"{src.stem}_clean.csv"
    else:
        out = Path(out_path).resolve()

    df.to_csv(str(out), index=False, encoding="utf-8")

    # --------------------------------------------------------------- Report
    null_summary = {
        col: int(df[col].isna().sum())
        for col in df.columns
        if df[col].isna().sum() > 0
    }

    numeric_summary: Dict[str, Dict] = {}
    for col in df.select_dtypes(include=[np.number]).columns:
        s = df[col].dropna()
        if len(s) < MIN_VALID:
            continue
        numeric_summary[col] = {
            "min":    round(float(s.min()), 4),
            "max":    round(float(s.max()), 4),
            "mean":   round(float(s.mean()), 4),
            "median": round(float(s.median()), 4),
            "std":    round(float(s.std()), 4),
            "nulls":  int(df[col].isna().sum()),
        }

    report = {
        "source":            str(src),
        "output":            str(out),
        "original_shape":    {"rows": original_shape[0], "cols": original_shape[1]},
        "final_shape":       {"rows": len(df), "cols": len(df.columns)},
        "columns":           list(df.columns),
        "dtype_map":         {col: str(dtype) for col, dtype in df.dtypes.items()},
        "remaining_nulls":   null_summary,
        "numeric_summary":   numeric_summary,
        "high_card_flagged": high_card_cols,
        "steps_applied":     log,
        "warnings":          warnings_list,
        "scaling_note": (
            "StandardScaler / MinMaxScaler NOT applied. "
            "generator.py uses quantile-based inverse CDF on original scale — "
            "pre-scaling would corrupt synthetic output without de-normalization."
        ),
    }

    if verbose:
        print(json.dumps(report, indent=2, ensure_ascii=False))

    return CleanResult(
        output_path=str(out),
        original_shape=original_shape,
        final_shape=(len(df), len(df.columns)),
        report=report,
    )


# ---------------------------------------------------------------------------
# Convenience: clean + adapt in one call
# ---------------------------------------------------------------------------

def clean_and_adapt(
    input_path: str,
    *,
    target_col: Optional[str] = None,
    max_rows: int = 5_000,
    out_path: Optional[str] = None,
    outlier_multiplier: float = DEFAULT_IQR_MULTIPLIER,
    drop_high_cardinality: bool = True,
    encoding: str = "utf-8",
    verbose: bool = True,
) -> str:
    """
    Full pre-processing pipeline:
        raw CSV  →  clean()  →  adapt()  →  extension-ready CSV

    Returns the final output path.
    """
    # Step 1: clean
    clean_out = str(Path(input_path).parent / f"{Path(input_path).stem}_clean.csv")
    clean(
        input_path,
        out_path=clean_out,
        outlier_multiplier=outlier_multiplier,
        drop_high_cardinality=drop_high_cardinality,
        encoding=encoding,
        verbose=verbose,
    )

    # Step 2: adapt (sample + target detection)
    try:
        from data_adapter import adapt
    except ImportError:
        from src.utils.data_adapter import adapt  # type: ignore

    final = adapt(
        clean_out,
        output_path=out_path,
        target_col=target_col,
        max_rows=max_rows,
        do_sample=True,
        verbose=verbose,
    )

    # Remove intermediate
    if os.path.exists(clean_out) and clean_out != final:
        os.remove(clean_out)

    return final


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="data_cleaner",
        description="Clean a raw CSV for Aurora extension ingestion.",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    p.add_argument("input", help="Source CSV path.")
    p.add_argument("--out", metavar="PATH", help="Output path (default: <stem>_clean.csv).")
    p.add_argument("--outlier-multiplier", type=float, default=DEFAULT_IQR_MULTIPLIER,
                   metavar="K",
                   help=f"IQR fence multiplier for outlier capping (default: {DEFAULT_IQR_MULTIPLIER}).")
    p.add_argument("--drop-high-cardinality", action="store_true",
                   help="Drop suspected ID/hash columns instead of just flagging them.")
    p.add_argument("--no-impute", action="store_true",
                   help="Skip null imputation.")
    p.add_argument("--no-cap", action="store_true",
                   help="Skip outlier capping.")
    p.add_argument("--no-datetime", action="store_true",
                   help="Skip datetime ISO conversion.")
    p.add_argument("--encoding", default="utf-8",
                   help="Source encoding (default: utf-8).")
    p.add_argument("--quiet", action="store_true",
                   help="Suppress JSON report.")
    p.add_argument("--report", action="store_true",
                   help="Print JSON report and exit (do not write output).")
    return p


def _main(argv=None) -> int:
    args = _build_parser().parse_args(argv)
    try:
        result = clean(
            args.input,
            out_path=args.out,
            outlier_multiplier=args.outlier_multiplier,
            drop_high_cardinality=args.drop_high_cardinality,
            impute_nulls=not args.no_impute,
            cap_outliers=not args.no_cap,
            fix_datetimes=not args.no_datetime,
            encoding=args.encoding,
            verbose=not args.quiet,
        )
        if args.report:
            result.print_report()
        return 0
    except FileNotFoundError as e:
        print(f"ERROR: {e}", file=sys.stderr)
        return 1
    except Exception as e:
        print(f"UNEXPECTED ERROR: {e}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(_main())
