.PHONY: test quality verify package serve

test:
	PYTHONPATH=src:. python -m unittest discover -s tests -v

quality:
	python scripts/quality.py
	python -m compileall -q src tests scripts

verify: quality test
	python scripts/verify_manifest.py

package:
	python -m pip wheel . --no-deps --no-build-isolation -w dist

serve:
	PYTHONPATH=src python -m chronosqueue.cli serve
