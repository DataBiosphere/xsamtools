include common.mk

MODULES=xsamtools tests
SAMTOOLS_VERSION=1.15.1

export TNU_TESTMODE?=workspace_access

test: lint mypy test-samtools tests

lint:
	flake8 $(MODULES) *.py

mypy:
	mypy --ignore-missing-imports --no-strict-optional $(MODULES)

tests:
	PYTHONWARNINGS=ignore:ResourceWarning coverage run --source=xsamtools \
		-m unittest discover --start-directory tests --top-level-directory . --verbose

package_samtools: clean_samtools samtools htslib bcftools

samtools:
	wget https://github.com/samtools/samtools/releases/download/$(SAMTOOLS_VERSION)/samtools-$(SAMTOOLS_VERSION).tar.bz2 -O samtools.tar.bz2

htslib:
	wget https://github.com/samtools/htslib/releases/download/$(SAMTOOLS_VERSION)/htslib-$(SAMTOOLS_VERSION).tar.bz2 -O htslib.tar.bz2

bcftools:
	wget https://github.com/samtools/bcftools/releases/download/$(SAMTOOLS_VERSION)/bcftools-$(SAMTOOLS_VERSION).tar.bz2 -O bcftools.tar.bz2

clean_samtools:
	rm -rf samtools.tar.bz2 bcftools.tar.bz2 htslib.tar.bz2

version: xsamtools/version.py

xsamtools/version.py: setup.py
	echo "__version__ = '$$(python setup.py --version)'" > $@

sdist: version
	python setup.py sdist

build: version
	python setup.py bdist_wheel

install: build
	pip install --upgrade dist/*.whl

test-samtools: build/samtools/samtools build/htslib/htsfile build/bcftools/bcftools
build/htslib/htsfile:
	python setup.py bdist_wheel
build/bcftools/bcftools:
	python setup.py bdist_wheel
build/samtools/samtools:
	python setup.py bdist_wheel

.PHONY: test lint mypy tests sdist build install package_samtools
