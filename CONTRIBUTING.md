# 🤝 Contributing to Auto Mate

Thank you for contributing! This guide covers the branching strategy, commit conventions, and PR process for the NNA Team.

---

## Branching Strategy

```
main
  └── dev
       ├── feature/<short-name>
       ├── fix/<short-name>
       └── docs/<short-name>
```

- `main` — stable, releasable code only
- `dev` — integration branch; all features merge here first
- `feature/*` — new features
- `fix/*` — bug fixes
- `docs/*` — documentation only

**Never push directly to `main` or `dev`.** Always open a PR.

---

## Getting Started

```bash
# 1. Fork and clone
git clone https://github.com/NNA-team/automate.git
cd automate

# 2. Create your branch from dev
git checkout dev
git pull origin dev
git checkout -b feature/your-feature-name

# 3. Install dependencies
npm install
pip install -r requirements.txt

# 4. Make your changes, then build
npm run compile

# 5. Launch the extension in dev mode
# Press F5 in VS Code
```

---

## Commit Message Convention

Follow the [Conventional Commits](https://www.conventionalcommits.org/) format:

```
<type>(<scope>): <short description>

[optional body]
```

**Types:**

| Type | When to Use |
|------|-------------|
| `feat` | A new feature |
| `fix` | A bug fix |
| `docs` | Documentation changes only |
| `refactor` | Code restructuring, no behavior change |
| `test` | Adding or fixing tests |
| `chore` | Build scripts, config, dependencies |
| `perf` | Performance improvements |

**Examples:**

```bash
feat(generator): add per-class Gaussian copula for ProbabilisticEngine
fix(extension): register idelense.generateSynthetic command
docs(api): add baseline.py API reference
chore(deps): bump webpack to 5.104.1
```

---

## Pull Request Process

1. Make sure your branch is up to date with `dev`
   ```bash
   git fetch origin
   git rebase origin/dev
   ```

2. Run the build and check for TypeScript errors
   ```bash
   npm run compile
   npm run lint
   ```

3. Open a PR against `dev` (not `main`)

4. Fill in the PR template (description, type of change, testing done)

5. Request a review from at least one other team member

6. Do **not** merge your own PR — wait for approval

---

## Code Style

### TypeScript
- All VS Code extension code lives in `src/extension.ts`
- Use `async/await` over `.then()` chains
- Prefer `const` over `let`
- Handle errors with `try/catch` — never silently swallow errors
- Use `vscode.window.showErrorMessage()` for user-facing errors

### Python
- Follow PEP 8
- Use type hints for all function signatures
- Use `dataclasses` for structured data
- All public functions must have a docstring
- Prefer `Optional[X]` over `X | None` for Python < 3.10 compatibility

---

## Testing

### TypeScript
```bash
npm run compile-tests
npm run test
```

### Python
```bash
# Quick CLI test for parse.py
python src/utils/parse.py data/sample.csv

# Quick CLI test for baseline.py
python src/utils/baseline.py data/sample.csv --kind csv

# Full pipeline test
python src/utils/generator.py data/sample.csv cache/baseline.json --n 100
```

---

## File Placement

| What | Where |
|------|-------|
| Python backend scripts | `src/utils/` |
| TypeScript extension code | `src/` |
| Feature documentation | `docs/features/` |
| Architecture docs | `docs/` |
| Planning files | `planning/` |
| GitHub templates | `.github/` |
