INTO-SUBMODULE := cd ./airflow-docker-dev-setup

up:
	git submodule init
	git submodule update
	$(INTO-SUBMODULE) && $(MAKE) up

stop:
	$(INTO-SUBMODULE) && $(MAKE) stop

reload:
	$(INTO-SUBMODULE) && $(MAKE) reload

down:
	$(INTO-SUBMODULE) && $(MAKE) down

tty-webserver:
	$(INTO-SUBMODULE) && $(MAKE) tty-webserver

tty-worker:
	$(INTO-SUBMODULE) && $(MAKE) tty-worker

tty-scheduler:
	$(INTO-SUBMODULE) && $(MAKE) tty-scheduler

tty-root-worker:
	$(INTO-SUBMODULE) && $(MAKE) tty-root-worker

tty-root-webserver:
	$(INTO-SUBMODULE) && $(MAKE) tty-root-webserver

tty-root-scheduler:
	$(INTO-SUBMODULE) && $(MAKE) tty-root-scheduler

load-vars:
	$(INTO-SUBMODULE) && $(MAKE) load-vars

compare-dependencies:
	.github/shared-scripts/compare_dependencies.sh

lint:
	pipenv run pylint funcake_dags -E

test:
	PYTHONPATH=. pipenv run pytest

build-requirements:
	.github/shared-scripts/build-requirements.sh

rebuild-pipfile: build-requirements
	pipenv --rm
	rm -f Pipfile.lock
	pipenv install --dev --requirements pipfile-requirements.txt
