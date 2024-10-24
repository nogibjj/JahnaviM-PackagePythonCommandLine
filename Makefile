install:
	pip install --upgrade pip && pip install -r requirements.txt

format:
	black *.py

lint:
	pylint --ignore-patterns=test_.*?py *.py
	ruff check *.py

test:
	python -m pytest -cov=mylib -cov=etl test_*.py

all:
	install format lint test
