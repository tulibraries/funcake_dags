#!/usr/bin/env bash
set -e pipefail

# This file is used to apply an xslt tranformation to some source files and
# push them to a configured s3 bucket.

SAXON_VERSION=9.9.1-5
SAXON_DOWNLOAD_SHA1=c1f413a1b810dbf0d673ffd3b27c8829a82ac31c
SAXON_CP=/tmp/saxon/saxon-$SAXON_VERSION.jar

if [ ! -f $SAXON_CP ]; then
	mkdir -p /tmp/saxon && \
		curl -fSL -o ${SAXON_CP} http://central.maven.org/maven2/net/sf/saxon/Saxon-HE/${SAXON_VERSION}/Saxon-HE-${SAXON_VERSION}.jar && \
		echo ${SAXON_DOWNLOAD_SHA1} ${SAXON_CP} | sha1sum -c - && \
		chmod +x ${SAXON_CP}
fi

# Generate XSL URL similarly to proposal for SCHEMATRON Python
XSL=https://raw.githubusercontent.com/${XSL_REPO}/${XSL_BRANCH}/${XSL_FILENAME}
echo Transformation File: $XSL

# Grab list of items from designated aws bucket (creds are envvars), then index each item
RESP=`aws s3api list-objects --bucket $BUCKET --prefix ${DAG_ID}/${DAG_TS}/${SOURCE}`
for SOURCE_XML in `echo $RESP | jq -r '.Contents[].Key'`
do
  SOURCE_URL=$(aws s3 presign s3://$BUCKET/$SOURCE_XML)
  echo Reading from $SOURCE_URL

  # Transform source xml and pipe to s3 bucket.
  TRANSFORM_XML=$(echo $SOURCE_XML | sed -e "s/$SOURCE/$DEST/g")
  echo Writing to $TRANSFORM_XML

	java -jar $SAXON_CP -xsl:$XSL -s:$SOURCE_URL -o:$SOURCE_XML-1.xml

	sed -e "s|<?xml version=.*?>|<collection dag-id='$DAG_ID' dag-timestamp='$DAG_TS'>|g" $SOURCE_XML-1.xml > $SOURCE_XML-2.xml
	echo "</collection>" >> $SOURCE_XML-2.xml

	java -jar $SAXON_CP -xsl:$SCRIPTS_PATH/batch-transform.xsl -s:$SOURCE_XML-2.xml | aws s3 cp - s3://$BUCKET/$TRANSFORM_XML
done
