# 📊 PCA Evaluation Report: Synthetic vs Real Network Traffic Data
### Dataset: UNSW-NB15 | Engine: Synthetic Data Generator

---

## 📌 Overview

This report evaluates the fidelity of a **synthetic network traffic data generation engine** by comparing its output against real samples from the [UNSW-NB15 dataset](https://research.unsw.edu.au/projects/unsw-nb15-dataset) — a widely used benchmark for network intrusion detection research.

Evaluation was performed using **Principal Component Analysis (PCA)**, projecting both real and synthetic data into 2D space to visually and statistically assess distributional alignment across **4 independent test runs**.

- 🔵 **Blue Dots** → Real Data (UNSW-NB15 Sample)
- 🔴 **Red ×** → Synthetic Data (Engine Output)

> **Ideal outcome:** Red and blue points overlap significantly, indicating the synthetic engine reproduces the statistical structure of real traffic data.

---

## 🧪 Test Run Results

### Run 1 — `PC1: 37.23%` | `PC2: 12.97%`

![Run 1](./assets/run1.png)

| Metric | Observation |
|---|---|
| PC1 Variance | **Highest** across all runs (37.23%) |
| PC2 Variance | **Lowest** across all runs (12.97%) |
| Central Cluster Overlap | Moderate |
| Outlier Coverage | Poor |
| Synthetic Spread Pattern | Mid-range concentrated |

**Key Findings:**
- The engine captures the dominant PC1 dimension well, evidenced by the highest first-component variance.
- Real data exhibits isolated outliers at extreme coordinates `(5.5, 10)`, `(6.5, −3.8)`, and `(4.8, −3.6)` — none of which are reproduced by the synthetic engine.
- Synthetic points cluster in the `x = −3 to 0` mid-range but do not follow the real data's extended spread.
- The very low PC2 (12.97%) suggests the engine heavily compressed secondary variance in this run — a sign of **dimensional underfitting**.

---

### Run 2 — `PC1: 32.68%` | `PC2: 20.34%`

![Run 2](./assets/run2.png)

| Metric | Observation |
|---|---|
| PC1 Variance | Second lowest (32.68%) |
| PC2 Variance | **Highest** across all runs (20.34%) |
| Central Cluster Overlap | **Best** across all runs |
| Outlier Coverage | Poor |
| Synthetic Spread Pattern | Diagonal downward-right drift |

**Key Findings:**
- This run achieved the **most balanced variance split** (32.68% / 20.34%), indicating the engine successfully captured 2D structural complexity in this instance.
- The central cluster `(x = −3 to −1)` shows the **tightest real-vs-synthetic co-location** of all 4 runs.
- A diagonal pattern in synthetic output `(x = 0 to 5, y = −1 to −4)` partially tracks real data in the lower-right quadrant — a positive behavioral signal.
- Extreme real outliers at `(7, 11)` and `(3.8, 6.8)` remain unmatched.
- **Rated: Best performing run overall.**

---

### Run 3 — `PC1: 28.20%` | `PC2: 19.31%`

![Run 3](./assets/run3.png)

| Metric | Observation |
|---|---|
| PC1 Variance | **Lowest** across all runs (28.20%) |
| PC2 Variance | Second highest (19.31%) |
| Central Cluster Overlap | Good |
| Outlier Coverage | Partial (one near-match) |
| Synthetic Spread Pattern | Diagonal mid-to-lower spread |

**Key Findings:**
- Lowest PC1 variance indicates this run distributed explained variance most evenly — suggesting the engine didn't latch onto a single dominant axis, which can indicate **more generalised generation**.
- One synthetic point near `(2.2, −3.9)` closely matches a real data point in the same region — the **only notable outlier approximation** across all 4 runs.
- Synthetic data again follows a downward-right diagonal, concentrated between `x = 0` to `3`, `y = −1` to `−3`.
- Real outliers at `(8.3, 9.7)` and `(5.7, 5.6)` remain unreached.
- Overall alignment is reasonable but diffuse.

---

### Run 4 — `PC1: 36.91%` | `PC2: 15.42%`

![Run 4](./assets/run4.png)

| Metric | Observation |
|---|---|
| PC1 Variance | Second highest (36.91%) |
| PC2 Variance | Second lowest (15.42%) |
| Central Cluster Overlap | Good |
| Outlier Coverage | Poor |
| Synthetic Spread Pattern | Wide horizontal (PC1-axis biased) |

**Key Findings:**
- Variance profile closely mirrors Run 1 (PC1 ~37%, PC2 ~15%), suggesting the engine converges to a similar internal state under certain conditions.
- Synthetic × marks are broadly spread along the horizontal PC1 axis `(x = −4 to 6)` but remain flattened near `y ≈ 0 to 1`.
- Real data has a dramatic outlier at `(1.3, 11.9)` — the highest PC2 value in any run — completely absent from synthetic output.
- **PC2 underfitting is most visible here**: the engine nearly ignores vertical spread.

---

## 📈 Cross-Run Comparison

| Run | PC1 Variance | PC2 Variance | Total Explained | Central Overlap | Outlier Coverage | Rating |
|---|---|---|---|---|---|---|
| **Run 1** | 37.23% | 12.97% | 50.20% | Moderate | ❌ Poor | ⭐⭐ |
| **Run 2** | 32.68% | 20.34% | 53.02% | ✅ Best | ❌ Poor | ⭐⭐⭐⭐ |
| **Run 3** | 28.20% | 19.31% | 47.51% | Good | ⚠️ Partial | ⭐⭐⭐ |
| **Run 4** | 36.91% | 15.42% | 52.33% | Good | ❌ Poor | ⭐⭐⭐ |

> **Total Explained Variance** = PC1 + PC2 combined. Higher is better for 2D representation fidelity.

---

## ⚠️ Identified Issues

### 1. 🔴 Outlier Blindspot (Critical)
Across **all 4 runs**, the synthetic engine fails to generate samples that correspond to real data's extreme-valued points. These outliers may represent rare but critical attack patterns in network traffic — their absence is a significant **fidelity and security gap**.

```
Unmatched Real Outliers (examples):
  Run 1: (5.5, 10.0), (6.5, −3.8)
  Run 2: (7.0, 11.0), (3.8, 6.8)
  Run 3: (8.3, 9.7), (5.7, 5.6)
  Run 4: (1.3, 11.9), (7.5, −4.5)
```

### 2. 🟡 Variance Instability (Moderate)
PC1 variance fluctuates between **28.20% – 37.23%** across runs. This indicates the engine does not produce stable distributional outputs — synthetic data behaves differently each run without a clear convergence.

### 3. 🟡 PC2 Compression (Moderate)
The synthetic engine consistently clusters near `PC2 ≈ 0`, while real data shows meaningful spread along the vertical axis. The engine is likely **underfitting the second principal dimension**, meaning it misses secondary structural patterns in the data.

### 4. 🟢 Central Region Performance (Acceptable)
In the dense central cluster (high-frequency normal traffic patterns), overlap between real and synthetic is consistently adequate — particularly strong in Run 2. This suggests the engine handles common traffic patterns well.

---

## 💡 Recommendations

| Priority | Recommendation |
|---|---|
| 🔴 High | Implement **outlier-aware sampling** (e.g., boundary sampling, tail augmentation) to ensure rare but real patterns are represented in synthetic output |
| 🔴 High | Add **variance stability checks** between runs — enforce consistency via seeding or distribution constraints |
| 🟡 Medium | Introduce a **PC2 alignment loss** or secondary-axis fidelity metric during engine training/tuning |
| 🟡 Medium | Evaluate using **t-SNE or UMAP** alongside PCA for non-linear structure comparison |
| 🟢 Low | Consider increasing sample size per run to reduce visual noise and improve statistical confidence |

---

## 🏆 Best Run

> **Run 2** is the highest-fidelity synthetic output with the best central cluster alignment, highest PC2 variance (20.34%), and most balanced overall explained variance (53.02%).

---

## 🛠️ Methodology

- **Dataset:** UNSW-NB15 (network intrusion benchmark)
- **Dimensionality Reduction:** PCA (2 components)
- **Evaluation:** Visual distributional comparison across 4 independent engine runs
- **Metrics Used:** Explained variance ratio (PC1, PC2), cluster overlap, outlier coverage

---

## 📁 Repository Structure (Suggested)

```
├── data/
│   ├── real/               # UNSW-NB15 samples
│   └── synthetic/          # Engine output per run
├── assets/
│   ├── run1.png
│   ├── run2.png
│   ├── run3.png
│   └── run4.png
├── evaluation/
│   └── pca_comparison.py   # PCA plotting script
├── PCA_Synthetic_vs_Real_Report.md   ← this file
└── README.md
```

---

*Report generated from 4 PCA test runs. All plots produced using matplotlib with scikit-learn PCA decomposition on standardised feature vectors.*
