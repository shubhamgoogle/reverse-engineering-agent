install:
	@poetry config virtualenvs.in-project true; poetry install

pre-commit:
	@pre-commit run --all-files

pytest:
	pytest

.PHONY: dev dev-backend dev-frontend install pre-commit pytest

# --- Local Development ---

dev:
	@echo "Starting backend and frontend services..."
	@bash start.sh

dev-backend:
	@echo "Starting backend service..."
	poetry run uvicorn src.main:app --reload --port=8000

dev-frontend:
	@echo "Starting frontend service..."
	poetry run streamlit run src/frontend/app.py
