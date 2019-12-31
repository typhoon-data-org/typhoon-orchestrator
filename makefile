.PHONY: clean

clean:
	rm -rf build/ dist/ *.egg-info

build-orchestrator: clean
	python3 setup.py sdist bdist_wheel

publish-docs:
	mkdocs gh-deploy

publish-orchestrator: build-orchestrator
	python -m twine upload --repository-url https://upload.pypi.org/legacy/ dist/*

build-webserver: clean
	python3 webserver/setup.py sdist bdist_wheel

publish-webserver: build-webserver
	python -m twine upload --repository-url https://upload.pypi.org/legacy/ dist/*
