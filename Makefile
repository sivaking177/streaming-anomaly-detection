# Common commands. Assumes the project venv is at ../.venv (shared across
# the two projects in the workspace) or a local .venv; override with
# PYTHON=... if yours lives elsewhere.
PYTHON ?= $(shell [ -x ../.venv/bin/python ] && echo ../.venv/bin/python || echo .venv/bin/python)

.PHONY: test sim dashboard lint types

test:
	$(PYTHON) -m pytest -q

sim:
	$(PYTHON) -m streamdetect.jobs.simulation --out ./out \
		--windows 40 --burst-start 25 --burst-multiplier 3

dashboard: sim
	$(PYTHON) -m streamdetect.output.dashboard --sink-dir ./out --port 8765

lint:
	$(PYTHON) -m black --check streamdetect tests
	$(PYTHON) -m ruff check streamdetect tests

types:
	$(PYTHON) -m mypy streamdetect
