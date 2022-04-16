# Dependencies: git pandoc moreutils httpie twine

SHELL=/bin/bash -eo pipefail
  
ifndef XSAMTOOLS_HOME
$(error Please run "source environment" in the xvcfmerge repo root directory before running make commands)
endif

release_major:
	$(eval export TAG=$(shell git describe --tags --match 'v*.*.*' | perl -ne '/^v(\d)+\.(\d)+\.(\d+)+/; print "v@{[$$1+1]}.0.0"'))
	$(MAKE) release

release_minor:
	$(eval export TAG=$(shell git describe --tags --match 'v*.*.*' | perl -ne '/^v(\d+)\.(\d+)\.(\d+)+/; print "v$$1.@{[$$2+1]}.0"'))
	$(MAKE) release

release_patch:
	$(eval export TAG=$(shell git describe --tags --match 'v*.*.*' | perl -ne '/^v(\d)+\.(\d)+\.(\d+)+/; print "v$$1.$$2.@{[$$3+1]}"'))
	$(MAKE) release

release: clean
	@if [[ $$(which twine) ]]; then :; else echo "*** Please install dependencies with 'pip install -r requirements-dev.txt' ***"; exit 1; fi
	@if [[ -z $$TAG ]]; then echo "Use release_{major,minor,patch}"; exit 1; fi
	git pull
	TAG_MSG=$$(mktemp); \
	    echo "# Changes for ${TAG} ($$(date +%Y-%m-%d))" > $$TAG_MSG; \
	    git log --pretty=format:%s $$(git describe --abbrev=0)..HEAD >> $$TAG_MSG; \
	    $${EDITOR:-vi} $$TAG_MSG; \
	    echo ${TAG} | sed 's/v\(.*\)/__version__ = "\1"\n/' > xsamtools/version.py; git add xsamtools/version.py; \
	    if [[ -f Changes.md ]]; then cat $$TAG_MSG <(echo) Changes.md | sponge Changes.md; git add Changes.md; fi; \
	    if [[ -f Changes.rst ]]; then cat <(pandoc --from markdown --to rst $$TAG_MSG) <(echo) Changes.rst | sponge Changes.rst; git add Changes.rst; fi; \
	    git commit -m ${TAG}; \
	    git tag --annotate --file $$TAG_MSG ${TAG}
	git push --follow-tags
	$(MAKE) pypi_release

pypi_release:
	python setup.py sdist
	twine upload dist/*

image:
	$(eval export IMAGE_NAME=$(shell $(XSAMTOOLS_HOME)/docker_scripts/image_tag.sh))
	docker build -f $(XSAMTOOLS_HOME)/Dockerfile --build-arg XSAMTOOLS_DOCKER_USER --build-arg XSAMTOOLS_HOME -t $(IMAGE_NAME) .

image-force:
	$(eval export IMAGE_NAME=$(shell $(XSAMTOOLS_HOME)/docker_scripts/image_tag.sh))
	docker build --no-cache -f $(XSAMTOOLS_HOME)/Dockerfile --build-arg XSAMTOOLS_DOCKER_USER --build-arg XSAMTOOLS_HOME -t $(IMAGE_NAME) .

publish-image: image
	docker push $(IMAGE_NAME)

.PHONY: release image image-force publish-image
