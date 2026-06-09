# Project-local venv bootstrapped from ~/basenv (bypasses asdf shims).
VENV ?= .venv
PYTHON := $(abspath $(VENV))/bin/python
PIP := $(PYTHON) -m pip

BASEENV_PYTHON := $(HOME)/basenv/bin/python
VERSION ?= $(shell $(PYTHON) -c "import pjk.version; print(pjk.version.__version__)" 2>/dev/null)

.PHONY: help setup test lint clean build release dev-release

help:
	@echo "Available targets:"
	@echo "  make setup               Create .venv from basenv and install dev deps"
	@echo "  make test                Run pytest"
	@echo "  make lint                Run ruff and black check"
	@echo "  make clean               Remove build artifacts"
	@echo "  make build               Build sdist and wheel"
	@echo "  make release VERSION=X   Bump, build, upload to PyPI"
	@echo "  make dev-release VERSION=X  Build & upload dev/pre-release"

$(PYTHON):
	@test -x "$(BASEENV_PYTHON)" || { \
	  echo "ERROR: $(BASEENV_PYTHON) not found."; \
	  echo "This project uses ~/basenv to bootstrap .venv (asdf is ignored)."; \
	  exit 1; \
	}
	"$(BASEENV_PYTHON)" -m venv "$(VENV)"

setup: $(PYTHON)
	$(PIP) install -U pip
	$(PIP) install -e ".[dev,aws]" build twine

test: | $(PYTHON)
	$(PYTHON) -m pytest -q

lint: | $(PYTHON)
	$(PYTHON) -m ruff check src tests
	$(PYTHON) -m black --check src tests

clean:
	rm -rf build dist src/*.egg-info *.egg-info

build: clean | $(PYTHON)
	$(PYTHON) -m build
	$(PYTHON) -m twine check dist/*

release: clean | $(PYTHON)
	@if [ -z "$(VERSION)" ]; then \
	  echo "ERROR: VERSION not set. Use 'make release VERSION=0.6.0'"; \
	  exit 1; \
	fi
	$(PYTHON) tools/bump_version.py $(VERSION)
	$(PYTHON) -m build
	$(PYTHON) -m twine check dist/*
	$(PYTHON) -m twine upload dist/*
	git push origin main --tags

dev-release: clean | $(PYTHON)
	@if [ -z "$(VERSION)" ]; then \
	  echo "ERROR: VERSION not set. Use 'make dev-release VERSION=0.6.0.dev1'"; \
	  exit 1; \
	fi
	$(PYTHON) tools/bump_version.py $(VERSION)
	$(PYTHON) -m build
	$(PYTHON) -m twine check dist/*
	$(PYTHON) -m twine upload dist/*
	git push origin main --tags
