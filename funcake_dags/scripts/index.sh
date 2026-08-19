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

# Disable Traject's automatic commit on indexer shutdown so Airflow can
# perform one explicit Solr commit after all indexing tasks finish.
sed -i.bak 's/"solr_writer.commit_on_close": true/"solr_writer.commit_on_close": false/' lib/$INDEXER.rb
rm lib/$INDEXER.rb.bak

# grab list of items from designated aws bucket (creds are envvars), then index each item
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
if [ -n "$DATA" ]; then
  RESP=$(echo "$DATA" | jq -r '.[]')
else
  RESP=$(aws s3 ls "s3://$BUCKET/$FOLDER" | awk '{print $4}')
fi

if [ -z "$RESP" ]; then
  if [ -n "$DATA" ]; then
    echo "ERROR: no record sets provided in DATA"
  else
    echo "ERROR: no record sets found at s3://$BUCKET/$FOLDER"
  fi
  exit 1
fi

RESP_COUNT=$(echo $RESP | wc -w | tr -d '[:space:]')

i=0
for record_set in $RESP
do
  i=$((i+1))
  if [ -n "$DATA" ]; then
    source_key=$record_set
  else
    source_key=$FOLDER$record_set
  fi
  url=$(aws s3 presign "s3://$BUCKET/$source_key")
  bundle exec $INDEXER ingest "$url" 2>&1 | tee -a "$TEMPFILE"
  INGEST_COUNT=$(grep -c 'finished Traject::Indexer\#process:.*records in.*seconds' "$TEMPFILE" || true)
  if [ "$INGEST_COUNT" -ne "$i" ]; then
    echo "ERROR: no completion line for record set $record_set (batch $i of $RESP_COUNT, indexed $INGEST_COUNT)"
    exit 1
  fi
done
