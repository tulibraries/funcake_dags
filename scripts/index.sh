#/bin/bash --login

# required for picking up rbenv variables; source for VMs, export for docker.
export HOME=$AIRFLOW_USER_HOME
source ~/.bashrc
export PATH="$AIRFLOW_USER_HOME/.rbenv/shims:$AIRFLOW_USER_HOME/.rbenv/bin:$PATH"


# have any error in following cause bash script to fail
set -e pipefail
# export / set all environment variables passed here by task for pick-up by subprocess
set -aux


# grab the indexer (ruby / traject) & install related gems
git clone https://github.com/tulibraries/$INDEXER.git tmp/$INDEXER
cd tmp/$INDEXER
gem install bundler
bundle install

# grab list of items from designated aws bucket (creds are envvars), then index each item
RESP=`aws s3 ls s3://$BUCKET/$FOLDER | awk '{print $4}'`
for record_set in `echo $RESP`
do
  bundle exec $INDEXER ingest $(aws s3 presign s3://$BUCKET/$FOLDER$record_set)
done
