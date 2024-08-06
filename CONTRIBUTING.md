
```markdown
# Contributing to BI Functions

We welcome contributions from the team. Here are some guidelines to follow:

## Adding New Functions

1. Create a new directory under `bi_functions` for your function.
2. Add your function in a `.py` file.
3. Add a corresponding test in the `tests` directory.
4. Update the documentation (README.md) with details about your function.

## Code Style

- Follow PEP 8 guidelines for Python code.
- Write meaningful commit messages.

## Running Tests

Ensure all tests pass before submitting a pull request:

```sh
python -m unittest discover -s bi_functions/db_utils/tests