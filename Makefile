include common.mk

MODULES=xsamtools tests

export TNU_TESTMODE?=workspace_access

test: lint mypy test-samtools tests

lint:
	flake8 $(MODULES) *.py

mypy:
	mypy --ignore-missing-imports --no-strict-optional $(MODULES)

tests:
	PYTHONWARNINGS=ignore:ResourceWarning coverage run --source=xsamtools \
		-m unittest discover --start-directory tests --top-level-directory . --verbose

version: xsamtools/version.py

xsamtools/version.py: setup.py
	echo "__version__ = '$$(python setup.py --version)'" > $@

sdist: version
	python setup.py sdist

build: version
	python setup.py bdist_wheel

install: build
	pip install -e --upgrade dist/*.whl

test-samtools: build/samtools/samtools build/htslib/htsfile build/bcftools/bcftools
build/htslib/htsfile:
	python setup.py bdist_wheel
build/bcftools/bcftools:
	python setup.py bdist_wheel
build/samtools/samtools:
	python setup.py bdist_wheel

.PHONY: test lint mypy tests sdist build install
