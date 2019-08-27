# Funnel Cake DAGs

![pylint Score](https://mperlet.github.io/pybadge/badges/4.46.svg) [![Build Status](https://travis-ci.com/tulibraries/funcake_dags.svg?branch=qa)](https://travis-ci.com/tulibraries/funcake_dags)

This is the repository for Funnel Cake (PA Digital / DPLA Data QA Interface) Airflow DAGs (Directed Acyclic Graphs, e.g., data processing workflows) for data indexing to Solr + related jobs. These DAGs are expecting to be run within an Airflow installation akin to the one built by our [TUL Airflow Playbook (private repository)](https://github.com/tulibraries/ansible-playbook-airflow).

## Repository Structure

This repository has 3 main groups of files:
- Airflow DAG definition python files (ending with `_dag.py`);
- Airflow DAG tasks python files used by the above (starting with `task_`);
- and required local development, test, deployment, and CI files (`tests`, `configs`, `.travis`, Pipfile, etc.).

## Airflow Expectations

These the Airflow expectations for these Funnel Cake DAGs to successfully run:

**Libraries & Packages**

- Python 3.6 (limited by apache-airflow requirements)
- Python Packages: see the [Pipfile](Pipfile)

**Airflow Variables**

- `FUNCAKE_COLLECTION`: The SolrCloud Collection to be indexed to then aliased.

**Airflow Connections**


**Environment Variables**


**Infrastructure & Integration**

- Accessible DPLAH OAI-PMH API Endpoint
- Accessible Solr (SolrCloud) Endpoint

## Local Development

### Run with Airflow Playbook Make Commands

Basically, clone https://github.com/tulibraries/ansible_playbook_airflow locally; in a shell at the top level of that repository, run `make up` then `make update`; then git clone/pull your working funcake_dags code to ansible_playbook_airflow/dags/funcake-dags. This shows & runs your development DAG code in the Airflow docker containers, with a webserver at http://localhost:8080.

The steps for this, typed out:

```
$ cd ~/path/to/cloned/ansible-playbook-airflow
$ pipenv shell
(ansible-playbook-airflow) $ make up
# update `data/example-variables.json` to add whatever Airflow variables you want; you can also do this in the GUI if preferred.
# update `Makefile` to add whatever Airflow variables you want; you can also do this in the GUI if preferred.
# check http://localhost:8010 is running okay
(ansible-playbook-airflow) $ make update
```

Then change into `dags/funcake_dags` and `git pull origin your-working-branch` to get your working branch locally available. *Symlinks will not work due to how Airflow & Docker handle mounts.* You could manually copy over files if that works better for you.

Continuing from above:

```
(ansible-playbook-airflow) $ cd dags
(ansible-playbook-airflow) $ git clone https://github.com/tulibraries/funcake_dags.git
(ansible-playbook-airflow) $ cd funcake_dags
(ansible-playbook-airflow) $ git checkout -b my-working-branch
```

Changes to this DAGs folder will be reflected in the local Airflow web UI and related Airflow services within a few seconds. *DAG folders in the ansible-playbook-airflow directory will not be replaced, updated, or touched by any make commands if they already exist.*

When you're done with local development, git commit & push your changes to funcake_dags up to your working branch, and tear down the docker resources:

```
(ansible-playbook-airflow) $ make down
(ansible-playbook-airflow) $ exit # leaves the pipenv virtual environment
```

## Linting & Testing

How to run the `pylint` linter on this repository:

```
# Ensure you have the correct Python & Pip running:
$ python --version
  Python 3.6.8
$ pip --version
  pip 18.1 from /home/tul08567/.pyenv/versions/3.6.8/lib/python3.6/site-packages/pip (python 3.6)
# Install Pipenv:
$ pip install pipenv
  Collecting pipenv ...
# Install requirements in Pipenv; requires envvar:
$ SLUGIFY_USES_TEXT_UNIDECODE=yes pipenv install --dev
  Pipfile.lock not found, creating ...
# Run the linter:
$ pipenv run pylint funcake_dags
  ...
```

Linting for Errors only (`pipenv run pylint cob_datapipeline -E`) is run by Travis on every PR.

How to run pytests (unit tests, largely) on this repository (run by Travis on every PR):

```
$ pipenv run pytest
```

## Deployment

to be written up.
