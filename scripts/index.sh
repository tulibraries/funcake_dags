#/bin/bash --login

# required for picking up rbenv variables; source for VMs, export for docker.
export HOME=$AIRFLOW_USER_HOME
source ~/.bashrc
export PATH="$AIRFLOW_HOME/.rbenv/shims:$AIRFLOW_HOME/.rbenv/bin:$PATH"


# have any error in following cause bash script to fail
set -e
# export / set all environment variables passed here by task for pick-up by subprocess
set -aux


# grab the funnel cake indexer (ruby / traject) & instal related gems
git clone https://github.com/tulibraries/funnel_cake_index.git tmp/funnel_cake_index
cd tmp/funnel_cake_index
gem install bundler
bundle install

# grab list of items from designated aws bucket (creds are envvars), then index each item
for record_set in `aws s3api list-objects --bucket $BUCKET --prefix $FOLDER | jq -r '.Contents[].Key'`
do
  bundle exec funnel_cake_index ingest $(aws s3 presign s3://$BUCKET/$record_set)
done
