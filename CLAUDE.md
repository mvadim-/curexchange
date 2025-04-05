# Currency Exchange API - Development Guide

## Build & Run Commands
```
# Install dependencies
pip install -r requirements.txt

# Run the application
python api.py 

# Run all tests
python -m unittest discover test/

# Run specific test file
python -m unittest test/test_api.py
python -m unittest test/test_exchange_rates_service.py

# Run single test case
python -m unittest test.test_api.ApiTestCase.test_exchange_rates_endpoint_auth
```

## Code Style Guidelines

### Imports
- Group in order: standard library, third-party, local application
- Sort alphabetically within groups
- Use explicit imports

### Type Annotations
- Use type hints for all functions (parameters and return values)
- Define custom types (e.g., `RateData`, `NormalizedRates`) for clarity

### Naming & Formatting
- Variables/functions: `snake_case`
- Constants: `UPPER_SNAKE_CASE`
- Classes: `PascalCase`
- 4 spaces for indentation
- Max line length: 100 characters

### Error Handling
- Use specific exception types
- Always log exceptions with meaningful messages
- Include both error handling and logging in functions

### Documentation
- Use Google-style docstrings with Args, Returns, and Raises sections
- Comment complex logic, prefer self-documenting code