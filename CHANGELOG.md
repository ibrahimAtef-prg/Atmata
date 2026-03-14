# 📋 Changelog

All notable changes to Auto Mate will be documented here.

Format: [Semantic Versioning](https://semver.org/) — `MAJOR.MINOR.PATCH`

---

## [Unreleased]

### Added
- Full Python pipeline: `parse.py` → `baseline.py` → `generator.py` → `validation.py` → `checkp.py`
- VS Code CodeLens: "Parse Dataset (IDE Lense)" on dataset import lines
- Parse + Baseline Webview panel
- Synthetic data generation with three engines: Statistical, Probabilistic, CTGAN
- Three-stage ValidationLayer: ConstraintFilter, RowQualityFilter, DuplicatePreFilter
- Real-time Checkpoint Monitor panel (2s polling, progress bar, per-round table)
- Label/target column auto-detection
- Per-class conditional generation (label-first sampling)
- Gaussian copula with empirical CDF transform
- Cholesky copula correlation injection
- Model caching (probabilistic + CTGAN)
- Configurable Python path via `idelense.pythonPath` setting
- Atomic checkpoint file writes via `os.replace()`

### Commands
- `idelense.parseDataset` — Analyse Dataset
- `idelense.generateSynthetic` — Generate Synthetic Data
- `idelense.openCheckpoint` — Open Checkpoint Monitor

---

## [0.0.1] — Initial Development

- Extension scaffolded with TypeScript, Webpack, VS Code API
- Package name: `automate`
- Display name: `DataGenerator Agent`
- Activates on: `onLanguage:python`

---

*This project is a 2nd Year Final Project at Helwan International Technological University (HITU), AI Department.*
