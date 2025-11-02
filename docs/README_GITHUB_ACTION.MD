# GitHub Actions CI/CD Documentation

## 1. Overview

This project uses GitHub Actions for continuous integration (CI) to ensure code quality and consistency across the codebase. The CI pipeline runs automatically on pushes and pull requests to validate code changes.

**Key Features**:

- **Automated Linting**: Checks code quality and formatting
- **Fast Feedback**: Runs on every push and pull request
- **Easy to Extend**: Simple workflow structure for adding more checks
- **Cost-Effective**: Uses GitHub-hosted runners (free for public repos)

## 2. Workflow Structure

### 2.1 Current Workflows

The project includes the following GitHub Actions workflows:

```text
.github/workflows/
└── lint.yml    # Linting checks for code quality
```

### 2.2 Lint Workflow (`lint.yml`)

**Purpose**: Validates code quality and formatting using Ruff

**Triggers**:

- Push to `main` or `develop` branches
- Pull requests targeting `main` or `develop` branches

**Jobs**:

- `lint`: Runs Python code linting and formatting checks

**Tools Used**:

- **Ruff**: Fast Python linter and formatter (replaces black, flake8, isort, etc.)

## 3. Workflow Details

### 3.1 Lint Workflow Steps

The lint workflow performs the following steps:

1. **Checkout Code**: Retrieves the repository code
2. **Install uv**: Sets up the uv package manager
3. **Set up Python**: Configures Python 3.11 environment
4. **Install Dependencies**: Installs project dependencies including dev tools
5. **Run Ruff Check**: Validates code against linting rules
6. **Run Ruff Format Check**: Ensures code follows formatting standards

### 3.2 Configuration

The workflow uses the following configuration:

- **Python Version**: 3.11
- **Package Manager**: uv (latest version)
- **Linting Tool**: Ruff (latest compatible version)

## 4. Running Linting Locally

### 4.1 Install Dev Dependencies

```bash
# Install all dependencies including dev tools
uv sync --extra dev
```

### 4.2 Run Linting Checks

**Check for linting errors**:

```bash
uv run ruff check .
```

**Check formatting**:

```bash
uv run ruff format --check .
```

**Auto-fix linting issues**:

```bash
uv run ruff check --fix .
```

**Auto-format code**:

```bash
uv run ruff format .
```

### 4.3 Pre-commit Hook (Optional)

To run linting before each commit, you can use a pre-commit hook:

```bash
# Create pre-commit hook
cat > .git/hooks/pre-commit << 'EOF'
#!/bin/bash
uv run ruff check . && uv run ruff format --check .
EOF

chmod +x .git/hooks/pre-commit
```

## 5. Extending the CI Pipeline

### 5.1 Adding More Linting Tools

You can extend the lint workflow to include additional checks:

**Example: Add SQL linting**:

```yaml
- name: Install sqlfluff
  run: |
    uv pip install sqlfluff

- name: Run SQL linting
  run: |
    uv run sqlfluff lint duckdb_dbt/models/
```

**Example: Add YAML linting**:

```yaml
- name: Install yamllint
  run: |
    uv pip install yamllint

- name: Run YAML linting
  run: |
    uv run yamllint visivo/project.visivo.yml
```

### 5.2 Adding Tests

To add a test job:

```yaml
jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: astral-sh/setup-uv@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.11"
      - run: uv sync --extra dev
      - run: uv run pytest tests/
```

### 5.3 Adding dbt Compilation Check

To validate dbt models compile correctly:

```yaml
- name: Install dbt-duckdb
  run: |
    uv sync

- name: Check dbt compilation
  run: |
    cd duckdb_dbt
    uv run dbt compile
```

### 5.4 Matrix Testing

Test against multiple Python versions:

```yaml
strategy:
  matrix:
    python-version: ["3.10", "3.11", "3.12"]
steps:
  - uses: actions/setup-python@v5
    with:
      python-version: ${{ matrix.python-version }}
```

## 6. Workflow Status

### 6.1 Viewing Workflow Status

**On GitHub**:

- Navigate to the "Actions" tab in your repository
- View workflow runs and their status
- Click on a run to see detailed logs

**Badge** (add to README.md):

```markdown
![Lint](https://github.com/your-username/local_elt_pipeline/actions/workflows/lint.yml/badge.svg)
```

### 6.2 Understanding Workflow Results

- **Green Checkmark**: All checks passed ✅
- **Red X**: One or more checks failed ❌
- **Yellow Circle**: Workflow is running ⏳

## 7. Troubleshooting

### 7.1 Common Issues

**Issue**: Workflow fails with "uv: command not found"

**Solution**: Ensure the `setup-uv` action is included before using `uv`:

```yaml
- uses: astral-sh/setup-uv@v4
```

**Issue**: Ruff reports many errors on existing code

**Solution**:

1. Run `uv run ruff check --fix .` locally to auto-fix issues
2. Commit the fixes
3. Or add a `.ruff.toml` config file to customize rules

**Issue**: Formatting check fails

**Solution**: Run `uv run ruff format .` locally and commit the changes

### 7.2 Customizing Ruff Rules

Create a `.ruff.toml` file in the project root:

```toml
[tool.ruff]
line-length = 100
target-version = "py311"

[tool.ruff.lint]
select = ["E", "F", "I", "N", "W"]
ignore = ["E501"]  # Ignore line length errors

[tool.ruff.format]
quote-style = "double"
indent-style = "space"
```

### 7.3 Skipping CI (Use Sparingly)

To skip CI for a specific commit:

```bash
git commit -m "Update docs [skip ci]"
```

## 8. Best Practices

### 8.1 Commit Practices

1. **Run linting locally** before pushing:

   ```bash
   uv run ruff check . && uv run ruff format --check .
   ```

2. **Fix issues before committing**:

   ```bash
   uv run ruff check --fix . && uv run ruff format .
   ```

3. **Keep commits focused**: One logical change per commit

### 8.2 Workflow Best Practices

1. **Keep workflows fast**: Only run essential checks in CI
2. **Use caching**: Cache dependencies to speed up runs
3. **Fail fast**: Run quick checks first, slower checks later
4. **Document changes**: Update this README when adding new workflows

### 8.3 Dependency Management

- **Pin versions**: Use specific versions for reproducibility
- **Update regularly**: Keep dependencies up to date
- **Test updates**: Test dependency updates in a branch first

## 9. Advanced Configuration

### 9.1 Caching Dependencies

Speed up workflow runs by caching uv dependencies:

```yaml
- name: Cache uv dependencies
  uses: actions/cache@v3
  with:
    path: ~/.cache/uv
    key: ${{ runner.os }}-uv-${{ hashFiles('**/uv.lock') }}
    restore-keys: |
      ${{ runner.os }}-uv-
```

### 9.2 Conditional Workflow Execution

Run workflows only when specific files change:

```yaml
on:
  push:
    paths:
      - 'src/**'
      - 'tests/**'
      - 'pyproject.toml'
```

### 9.3 Workflow Dependencies

Run workflows in sequence:

```yaml
jobs:
  lint:
    # ... lint job ...
  
  test:
    needs: lint
    # ... test job ...
```

## 10. Additional Resources

- **GitHub Actions Documentation**: [https://docs.github.com/en/actions](https://docs.github.com/en/actions)
- **Ruff Documentation**: [https://docs.astral.sh/ruff/](https://docs.astral.sh/ruff/)
- **uv Documentation**: [https://github.com/astral-sh/uv](https://github.com/astral-sh/uv)
- **Workflow Syntax Reference**: [https://docs.github.com/en/actions/using-workflows/workflow-syntax-for-github-actions](https://docs.github.com/en/actions/using-workflows/workflow-syntax-for-github-actions)

## 11. Quick Reference

### 11.1 Local Linting Commands

```bash
# Check for issues
uv run ruff check .

# Fix auto-fixable issues
uv run ruff check --fix .

# Check formatting
uv run ruff format --check .

# Format code
uv run ruff format .
```

### 11.2 Workflow File Location

```text
.github/workflows/lint.yml
```

### 11.3 Workflow Status URL

Replace `your-username` and `repo-name`:

```
https://github.com/your-username/repo-name/actions/workflows/lint.yml
```
