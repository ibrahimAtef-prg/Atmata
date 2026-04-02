<div align="center">

# 🤖 Auto Mate — IDE Extension

### VS Code Extension for AI/ML Engineers & Data Scientists

[![VS Code](https://img.shields.io/badge/VS%20Code-%5E1.108.0-blue?logo=visualstudiocode)](https://code.visualstudio.com/)
[![TypeScript](https://img.shields.io/badge/TypeScript-5.x-3178C6?logo=typescript)](https://www.typescriptlang.org/)
[![Python](https://img.shields.io/badge/Python-3.9%2B-3776AB?logo=python)](https://python.org/)
[![Node.js](https://img.shields.io/badge/Node.js-18%2B-339933?logo=node.js)](https://nodejs.org/)
[![Webpack](https://img.shields.io/badge/Webpack-5.x-8DD6F9?logo=webpack)](https://webpack.js.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Status](https://img.shields.io/badge/Status-In%20Development-orange)]()

> **Auto Mate** is a VS Code extension built for ML engineers, AI researchers, and data scientists who need to augment, validate, and clean training data — all without leaving their editor. It detects dataset imports in your code, builds a behavioral baseline, generates privacy-safe synthetic data, and watches for leakage in real-time.

---

[Features](#-features) • [Architecture](#-architecture) • [Installation](#-installation) • [Usage](#-usage) • [Pipeline](#-pipeline) • [Team](#-team) • [Contributing](#-contributing)

</div>

---

## ✨ Features

### 🔍 Smart Dataset Detection (CodeLens)
Auto Mate scans your Python files for dataset import calls (`read_csv`, `read_excel`, `read_json`, `read_parquet`, `spark.read`) and surfaces a **"Parse Dataset (IDE Lense)"** CodeLens button inline — right above the line where you load your data.

### 📊 Dataset Parser & Schema Inspector
Powered by `parse.py` — a zero-dependency unified AST parser.
- Supports **CSV, Excel (.xlsx), JSON / JSONL, Parquet**
- Extracts schema, data types, null ratios, sample values, cardinality
- Computes a **SHA-256 fingerprint** for every dataset for traceability
- Full preview of the first N rows without loading the entire file

### 🧠 Behavioral Baseline Builder
Powered by `baseline.py` — builds a deep statistical contract for your dataset.
- Full quantile profile per column (q01 → q99), IQR, outlier bounds
- **Pearson correlations** between numeric columns
- **Cramér's V** for categorical–categorical associations
- **Point-biserial** for categorical–numeric associations
- Auto-detects label/target column (heuristic: highest total association with numeric features)
- Emits a machine-readable `BaselineArtifact` JSON and a human-readable rule set

### ⚗️ Synthetic Data Generator
Powered by `generator.py` — three engines, auto-selected by dataset size.

| Dataset Rows | Engine | Strategy |
|---|---|---|
| < 1,000 | **StatisticalEngine** | Quantile-CDF sampling + Cholesky copula |
| 1,000 – 50,000 | **ProbabilisticEngine** | Gaussian copula (empirical CDF → probit → MVN) |
| ≥ 50,000 | **CTGANEngine** | CTGAN (falls back to Probabilistic if not installed) |

All engines support:
- **Label-first generation** — sample class label first, then draw features from per-class distributions
- **Model caching** — trained models are serialised to disk, keyed by dataset fingerprint
- **Privacy-preserving** — DuplicatePreFilter ensures no generated row is an exact match of any training row

### ✅ Three-Stage Validation Layer
Powered by `validation.py` — every generated row passes through three independent gates.

1. **ConstraintFilter** — enforces numeric ranges and categorical allowed values via resample-retry (no clipping)
2. **RowQualityFilter** — IQR outer-fence (Tukey 3×), Mahalanobis coherence (chi² p=0.99), conditional label plausibility
3. **DuplicatePreFilter** — SHA-256 row-hash comparison against the original dataset

### 📡 Real-Time Checkpoint Monitor
Powered by `checkp.py` + a VS Code Webview panel — the generation monitor polls the checkpoint file every 2 seconds.
- Live progress bar (rows accepted / total requested)
- Per-round commit table: rows added, rejected (quality), rejected (dedup), repaired
- Full warnings log
- First 20 rows preview
- Atomic file writes — crash-safe JSON checkpoint store

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    VS Code Extension                     │
│                    (extension.ts)                        │
│                                                          │
│  CodeLensProvider → detectDataImports()                  │
│        │                                                 │
│        ▼                                                 │
│  idelense.parseDataset ──────────────────────────────┐   │
│        │                                             │   │
│        ▼                              Webview Panel  │   │
│  runPythonParser()  ◄──── parse.py        │          │   │
│  runBaseline()      ◄── baseline.py       │          │   │
│        │                                 │          │   │
│        ▼                                 │          │   │
│  showCombinedResult()  ──────────────────┘          │   │
│        │ (user clicks Generate)                     │   │
│        ▼                                            │   │
│  runGenerator() ◄──── generator.py                 │   │
│        │                    │                       │   │
│        │              ValidationLayer               │   │
│        │                    │                       │   │
│        │               CheckPoint  ◄─── Monitor     │   │
│        └─────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────┘
```

See full architecture details in [`docs/architecture.md`](docs/architecture.md).

---

## 🔁 Pipeline

```
Your Python File
      │
      │  pd.read_csv("data.csv")   ← CodeLens appears here
      ▼
 parse.py          → DatasetAST (schema, preview, fingerprint)
      │
      ▼
 baseline.py       → BaselineArtifact (stats, correlations, constraints, rules)
      │
      ▼
 generator.py      → engine selection → sample() → ValidationLayer
      │                                                    │
      │                            ┌─────────────────────┤
      │                            │  ConstraintFilter    │
      │                            │  RowQualityFilter    │
      │                            │  DuplicatePreFilter  │
      │                            └─────────────────────┘
      │                                    │
      ▼                                    ▼
 checkp.py         → CheckPoint.commit() → CheckPoint.seal()
      │
      ▼
 Monitor Panel     → polls every 2s → live progress + rows preview
```

---

## 🚀 Installation

### Prerequisites

| Dependency | Version | Purpose |
|---|---|---|
| VS Code | `^1.108.0` | IDE host |
| Node.js | `18+` | Extension build & runtime |
| Python | `3.9+` | Data processing backend |
| pandas | `>=1.5` | Baseline & parser |
| numpy | `>=1.23` | Generator math |
| openpyxl | `>=3.0` | Excel support |
| scipy | `>=1.9` | Gaussian copula (ProbabilisticEngine) |

### Install from Source

```bash
# 1. Clone the repository
git clone https://github.com/NNA-team/automate.git
cd automate

# 2. Install Node.js dependencies
npm install

# 3. Install Python dependencies
pip install -r requirements.txt

# 4. Build the extension
npm run compile

# 5. Launch in VS Code (opens Extension Development Host)
code .
# Press F5 to launch
```

### Optional: CTGAN (for large datasets ≥ 50,000 rows)

```bash
pip install ctgan
```

If ctgan is not installed, the extension automatically falls back to the ProbabilisticEngine with a warning.

---

## 🛠️ Usage

### 1. Analyse a Dataset
1. Open any `.py` file that loads data with pandas
2. Look for the **"Parse Dataset (IDE Lense)"** CodeLens above your import line
3. Click it — Auto Mate will run `parse.py` + `baseline.py` and open a results panel

### 2. Generate Synthetic Data
1. In the Parse+Baseline panel, set the number of rows
2. Click **Generate**
3. The Checkpoint Monitor panel opens — watch generation progress in real time
4. Once complete, the first 20 sample rows are shown; full output is saved to `.idelense/cache/`

### 3. Configure Python Path
If `python3` is not in your PATH:
```json
// .vscode/settings.json
{
  "idelense.pythonPath": "/usr/bin/python3"
}
```

### VS Code Commands (Command Palette)

| Command | Title |
|---|---|
| `idelense.parseDataset` | IDE Lense: Analyse Dataset |
| `idelense.generateSynthetic` | IDE Lense: Generate Synthetic Data |
| `idelense.openCheckpoint` | IDE Lense: Open Checkpoint Monitor |

---

## 📁 Repository Structure

```
automate/
├── src/
│   ├── extension.ts          # VS Code entry point, CodeLens, commands
│   └── utils/
│       ├── parse.py          # Dataset AST parser (CSV/Excel/JSON/Parquet/SQL)
│       ├── baseline.py       # Behavioral baseline builder
│       ├── generator.py      # Synthetic data generation engine
│       ├── validation.py     # Three-stage validation layer
│       └── checkp.py         # Atomic checkpoint store & monitor
├── docs/
│   ├── architecture.md       # Full system architecture
│   ├── api.md                # Python API reference
│   └── features/
│       ├── synthetic-data.md
│       ├── data-leakage.md
│       ├── dataset-explorer.md
│       └── checkpoint-monitor.md
├── planning/
│   ├── roadmap.md            # Project roadmap
│   └── sprints.md            # Sprint breakdown
├── .github/
│   ├── ISSUE_TEMPLATE/
│   │   ├── bug_report.md
│   │   └── feature_request.md
│   └── PULL_REQUEST_TEMPLATE.md
├── package.json              # Extension manifest
├── tsconfig.json             # TypeScript config
├── webpack.config.js         # Webpack bundler config
├── requirements.txt          # Python dependencies
├── CONTRIBUTING.md
├── CHANGELOG.md
└── .gitignore
```

---

## 👥 Team

**NNA Team** — Helwan International Technological University, AI Department

| # | Name | Student ID |
|---|------|-----------|
| 1 | Ibrahim Atef Mohamed Abdelfattah *(Leader)* | 2430404 |
| 2 | Ahmed Thrawat Mohamed Abdullah | 2430410 |
| 3 | Zeinab Mohamed Galal Morsy | 2430496 |
| 4 | Sara El-Sayed Mohamed Ibrahim | 2430497 |
| 5 | Somaya Alaa Abdelhalim Abdelaziz | 2430510 |
| 6 | Shorouk Magdy Esmat Ahmed Mohamed | 2430514 |
| 7 | Shereen Mohamed Ramadan Mohamed | 2430518 |
| 8 | Abdel-Rahman Mohamed Fahmy Abdel-Aal | 2430534 |
| 9 | Abdel-Rahman Farah Ahmed | 2430544 |
| 10 | Abdel-Rahman Mostafa Nabil Abdou Ahmed | 2430535 |
| 11 | Omar Ahmed Nady Mohamed Abdel-Salam | 2430565 |
| 12 | Omar Ayman Abdel-Aziz Abu El-Aal Farag | 2430566 |
| 13 | Mohamed Ahmed Mohamed Abdel-Aal El-Sayed | 2430601 |
| 14 | Malak Ihab Abdelhamid Abdelrahman | 2430665 |
| 15 | Mohamed Abdel-Nabi Mohamed Hammad | 2430615 |

**Supervisor:** *(Mohammed Ammar)*
**University:** Helwan International Technological University (HITU)
**Department:** Artificial Intelligence
**Year:** 2nd Year Final Project

---

## 🤝 Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for our branching strategy, commit conventions, and PR process.

---

## 📄 License

This project is licensed under the MIT License — see [LICENSE](LICENSE) for details.
