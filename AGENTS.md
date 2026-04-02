# AGENTS.md

## Testing Requirements
All changes must pass `python test_environment.py` before committing.
Any code added must pass `pytest tests/ -v`.

## Secrets Policy
Do not include API keys or sensitive data.
Never commit `.env` or any credentials.

## Scope Boundaries
Agents may edit files in src/ and notebooks/.
Do not modify requirements.txt without review.
Do not modify setup.sh without testing.

## Reproducibility Standard
All changes must run locally before committing.