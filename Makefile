VENV_DIR=pyvenv

PERF_DB=perfdb.sqlite3

DEFAULT=test

test: venv
	$(VENV_DIR)/bin/pytest

testx: venv
	$(VENV_DIR)/bin/pytest -x

clean:
	rm -rf $(VENV_DIR) $(PERF_DB)
	find . -type dir -name __pycache__ | xargs rm -rf
	find . -type dir -name databag.egg-info | xargs rm -rf

venv: $(VENV_DIR)/bin/activate
$(VENV_DIR)/bin/activate:
	python3 -m venv $(VENV_DIR)
	$(VENV_DIR)/bin/pip install -r reqs_dev.pip
	# setup this repo as an importable module in the virtualenv
	$(VENV_DIR)/bin/pip install -e .

perf: venv
	@ echo Running perf with file based db
	$(VENV_DIR)/bin/python src/tests/perf.py $(PERF_DB)
	@ echo Running perf with memory db
	$(VENV_DIR)/bin/python src/tests/perf.py ":memory:"
