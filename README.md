# orcabridge
Prototype of Orcapod as implemented in Python with functions

## Continuous Integration

This project uses GitHub Actions for continuous integration:

- **Run Tests**: A workflow that runs tests on Ubuntu with multiple Python versions.

### Running Tests Locally

To run tests locally:

```bash
# Install the package in development mode
pip install -e .

# Run tests with coverage
pytest -v --cov=src --cov-report=term-missing
```
