prepare:
	python -m pip install build twine flake8 pytest pytest-cov

build:
	python -m build

upload:
	python -m twine upload --repository pypi dist/*

lint:
	flake8 . --exclude venv --count --select=E9,F63,F7,F82 --show-source --statistics
	flake8 . --exclude venv --count --exit-zero --max-complexity=10 --max-line-length=127 --statistics

test:
	pytest --cov=src/promisipy tests/
