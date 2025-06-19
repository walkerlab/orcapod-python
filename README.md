# Orcapod Python
Orcapod's Python library for developing reproducbile scientific pipelines.

## Continuous Integration

This project uses GitHub Actions for continuous integration:

- **Run Tests**: A workflow that runs tests on Ubuntu with multiple Python versions.

### Running Tests Locally

To run tests locally:

```bash
# Install the package with test dependencies
pip install -e ".[test]"

# Run tests with coverage
pytest -v --cov=src --cov-report=term-missing
```

### Development Setup

For development, you can install all optional dependencies:

```bash
# Install all development dependencies 
pip install -e ".[test,dev]"
# or
pip install -r requirements-dev.txt
```
