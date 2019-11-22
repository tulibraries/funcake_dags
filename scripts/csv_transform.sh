#!/usr/bin/env bash
set -euxo pipefail

source ~/.bashrc
git clone https://tulibraries-devops:$GITHUB_TOKEN@github.com/tulibraries/aggregator_data.git
cd aggregator_data/$DAGID
cp $HOME/dags/funcake_dags/task_csv_transform.py .
python task_csv_transform.py

for file in $(find . -name "*.xml" -type f); do
  cat $file | aws s3 cp - s3://$BUCKET/$FOLDER/$(basename $file)
done
