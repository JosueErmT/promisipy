build:
	python -m build

upload:
	python -m twine upload --repository pypi dist/*

test:
	python -m unittest tests