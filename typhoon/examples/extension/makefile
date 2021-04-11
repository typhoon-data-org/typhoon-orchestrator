.PHONY: clean

clean:
	rm -rf build/ dist/ *.egg-info

build-extension: clean
	python3 setup.py sdist bdist_wheel

publish-extension: build-extension
	python -m twine upload --repository-url https://upload.pypi.org/legacy/ dist/*
