.PHONY: install test preprocess benchmark report run-all qa qa-check

install:
	pip install -e ".[dev]"

test:
	pytest tests/unit/ -v

preprocess:
	python -m cli.main preprocess --input "$(GRAPH_RAW_PATH)"

benchmark:
	python -m cli.main benchmark --fractions 100 --runs 3

report:
	python -m cli.main report

run-all: preprocess benchmark report

qa:
	@echo "Running full QA suite (informative, never aborts)..."
	bash scripts/run_qa.sh

qa-check:
	@echo "Running fast lint checks..."
	ruff check src/ || true
	pylint src/ --score=y || true
	bandit -r src/ -q || true
