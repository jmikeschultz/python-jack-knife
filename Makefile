# Variables
VERSION ?= $(shell python -c "import pjk.version; print(pjk.version.__version__)")

# Default target
help:
	@echo "Available targets:"
	@echo "  make test                Run pytest"
	@echo "  make lint                Run ruff and black check"
	@echo "  make clean               Remove build artifacts"
	@echo "  make build               Build sdist and wheel"
	@echo "  make release VERSION=X   Bump, build, upload to PyPI"
	@echo "  make dev-release VERSION=X  Build & upload dev/pre-release"

test:
	pytest -q

lint:
	ruff check src tests
	black --check src tests

clean:
	rm -rf build dist *.egg-info

build: clean
	python -m build
	twine check dist/*

release: clean
	@if [ -z "$(VERSION)" ]; then \
	  echo "ERROR: VERSION not set. Use 'make release VERSION=0.6.0'"; \
	  exit 1; \
	fi
	python tools/bump_version.py $(VERSION)
	python tools/create_configs_template.py "src/pjk" "src/pjk/resources/component_configs.tmpl"
	python -m build
	twine check dist/*
	twine upload dist/*
	git push origin main --tags

dev-release: clean
	@if [ -z "$(VERSION)" ]; then \
	  echo "ERROR: VERSION not set. Use 'make dev-release VERSION=0.6.0.dev1'"; \
	  exit 1; \
	fi
	python tools/bump_version.py $(VERSION)
	python -m build
	twine check dist/*
	twine upload dist/*
	git push origin main --tags
