#!/usr/bin/env bash
set -euxo pipefail

source ~/.bashrc

git clone https://tulibraries-devops:$GITHUB_TOKEN@github.com/tulibraries/aggregator_data.git
cd aggregator_data/$DAGID
cp $AIRFLOW_APP_HOME/dags/funcake_dags/scripts/csv_to_xml.py .
python csv_to_xml.py

for file in $(find . -name "*.xml" -type f); do
  cat $file | aws s3 cp - s3://$BUCKET/$FOLDER/$(basename $file)
done
