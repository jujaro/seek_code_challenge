#!/usr/bin/env make -f

install:
	python -m pip install -e . && \
	python setup.py build_ext --inplace

run-test:
	@python -m pytest

build-package:
	@python setup.py bdist_wheel

run:
	@python -m traffic_counter --data ./sample_data  2>/dev/null
