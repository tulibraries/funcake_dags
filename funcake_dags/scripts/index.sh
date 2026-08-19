#/bin/bash --login

# required for picking up rbenv variables; source for VMs, export for docker.
export HOME=$AIRFLOW_USER_HOME
source ~/.bashrc
export PATH="$AIRFLOW_USER_HOME/.rbenv/shims:$AIRFLOW_USER_HOME/.rbenv/bin:$PATH"


# have any error in following cause bash script to fail
set -eo pipefail
# export / set all environment variables passed here by task for pick-up by subprocess
set -aux


# grab the indexer (ruby / traject) & install related gems
git clone https://github.com/tulibraries/$INDEXER.git tmp/$INDEXER
cd tmp/$INDEXER
gem install bundler
bundle config set force_ruby_platform true
bundle install

TEMPFILE=$(mktemp /tmp/index-output.XXXXXX)
PUBLISH_TASK_REPORT=$AIRFLOW_HOME/dags/funcake_dags/scripts/publish_task_report.rb

report_and_cleanup() {
  rc=$?
  cat "$TEMPFILE" | ruby "$PUBLISH_TASK_REPORT" || true
  rm -f "$TEMPFILE" || true
  exit $rc
}
trap report_and_cleanup EXIT

# grab list of items from designated aws bucket (creds are envvars), then index each item
RESP=$(aws s3 ls "s3://$BUCKET/$FOLDER" | awk '{print $4}')
if [ -z "$RESP" ]; then echo "ERROR: no record sets found at s3://$BUCKET/$FOLDER"; exit 1; fi
RESP_COUNT=$(echo $RESP | wc -w | tr -d '[:space:]')

i=0
for record_set in $RESP
do
  i=$((i+1))
  url=$(aws s3 presign "s3://$BUCKET/$FOLDER$record_set")
  bundle exec $INDEXER ingest "$url" 2>&1 | tee -a "$TEMPFILE"
  INGEST_COUNT=$(grep -c 'finished Traject::Indexer\#process:.*records in.*seconds' "$TEMPFILE" || true)
  if [ "$INGEST_COUNT" -ne "$i" ]; then
    echo "ERROR: no completion line for record set $record_set (batch $i of $RESP_COUNT, indexed $INGEST_COUNT)"
    exit 1
  fi
done
