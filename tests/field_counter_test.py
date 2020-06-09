from funcake_dags.field_counter import field_count_report
import unittest
import boto3
from moto import mock_s3

class TestFieldCounter(unittest.TestCase):

    @mock_s3
    def test_field_count_report_csv_xml(self):
        bucket = "tulib-airflow-prod"
        key = "funcake_free_library_of_philadelphia/2019-12-12_17-17-40/new-updated/"
        conn = boto3.resource('s3', region_name='us-east-1')
        conn.create_bucket(Bucket=bucket)

        simple_xml="""
<collection dag-id="funcake_free_library_of_philadelphia" dag-timestamp="2019-12-12_17-17-40">
  <record airflow-record-id="1">
    <Item_No>frk00001</Item_No>
    <Title>Birth and Baptismal Certificate (Geburts und Taufschein) for Jonas Laber</Title>
    <Creator>Anonymous</Creator>
  </record>
  <record airflow-record-id="2">
    <Item_No>frk00001</Item_No>
    <Creator>Anonymous</Creator>
  </record>
</collection>
        """
        s3 = boto3.client('s3', region_name='us-east-1')
        s3.put_object(Bucket=bucket, Key=key+'simple_1.xml', Body=simple_xml)
        s3.put_object(Bucket=bucket, Key=key+'simple_2.xml', Body=simple_xml)
        s3.put_object(Bucket=bucket, Key=key+'simple_3.xml', Body=simple_xml)


        with self.assertLogs(level='INFO') as log:
            field_count_report(bucket, key)

        stats = "\n\n"
        stats += "Creator: |=========================|      6/6 | 100% \n"
        stats += "Item_No: |=========================|      6/6 | 100% \n"
        stats += "  Title: |============             |      3/6 |  50% \n"
        stats += "\n"
        stats += "        dc_completeness 16.666667\n"
        stats += "collection_completeness 83.333333\n"
        stats += "      wwww_completeness 0.000000\n"
        stats += "   average_completeness 33.333333\n"

        self.assertEqual.__self__.maxDiff = None
        self.assertEqual(["INFO:root:" + stats], log.output)

    @mock_s3
    def test_field_count_report_oai_pmh(self):
        bucket = "tulib-airflow-prod"
        key = "foobar/2019-12-12_17-17-40/new-updated/"
        conn = boto3.resource('s3', region_name='us-east-1')
        conn.create_bucket(Bucket=bucket)

        oai_xml="""
<oai:OAI-PMH xmlns:oai="http://www.openarchives.org/OAI/2.0/">
  <oai:ListRecords>
    <oai:record>
      <oai:header>
        <oai:identifier>oai:digital.klnpa.org:photograph/0</oai:identifier>
        <oai:datestamp>2017-10-27</oai:datestamp>
        <oai:setSpec>photograph</oai:setSpec>
      </oai:header>
      <oai:metadata>
        <oai_qdc:qualifieddcsi xmlns:oai_qdc="http://worldcat.org/xmlschemas/qdc-1.0/" xmlns:dc="http://purl.org/dc/elements/1.1/">
          <dc:title>foo</dc:title>
          <dc:subject>bar</dc:subject>
          <dc:creator>Pearson, J. R.</dc:creator>
        </oai_qdc:qualifieddcsi>
      </oai:metadata>
    </oai:record>
    <oai:record>
      <oai:header>
        <oai:identifier>oai:digital.klnpa.org:photograph/1</oai:identifier>
        <oai:datestamp>2017-10-27</oai:datestamp>
        <oai:setSpec>photograph</oai:setSpec>
      </oai:header>
      <oai:metadata>
        <oai_qdc:qualifieddcsi xmlns:oai_qdc="http://worldcat.org/xmlschemas/qdc-1.0/" xmlns:dc="http://purl.org/dc/elements/1.1/">
          <dc:subject>bar</dc:subject>
          <dc:creator>Pearson, J. R.</dc:creator>
        </oai_qdc:qualifieddcsi>
      </oai:metadata>
    </oai:record>
  </oai:ListRecords>
</oai:OAI-PMH>
        """
        s3 = boto3.client('s3', region_name='us-east-1')
        s3.put_object(Bucket=bucket, Key=key+'oai_1.xml', Body=oai_xml)
        s3.put_object(Bucket=bucket, Key=key+'oai_2.xml', Body=oai_xml)
        s3.put_object(Bucket=bucket, Key=key+'oai_3.xml', Body=oai_xml)


        with self.assertLogs(level='INFO') as log:
            field_count_report(bucket, key)

        stats = "\n\n"
        stats += "{http://purl.org/dc/elements/1.1/}creator: |=========================|      6/6 | 100% \n"
        stats += "{http://purl.org/dc/elements/1.1/}subject: |=========================|      6/6 | 100% \n"
        stats += "  {http://purl.org/dc/elements/1.1/}title: |============             |      3/6 |  50% \n"
        stats += "\n"
        stats += "        dc_completeness 16.666667\n"
        stats += "collection_completeness 83.333333\n"
        stats += "      wwww_completeness 37.500000\n"
        stats += "   average_completeness 45.833333\n"

        self.assertEqual.__self__.maxDiff = None
        self.assertEqual(["INFO:root:" + stats], log.output)

    @mock_s3
    def test_field_count_report_collection(self):
        bucket = "tulib-airflow-prod"
        key = "foobar/2019-12-12_17-17-40/transformed-filtered/"
        conn = boto3.resource('s3', region_name='us-east-1')
        conn.create_bucket(Bucket=bucket)

        coll_xml="""
<collection>
  <oai_dc:dc xmlns:dc="http://purl.org/dc/elements/1.1/" xmlns:dcterms="http://purl.org/dc/terms/" xmlns:oai_dc="http://www.openarchives.org/OAI/2.0/oai_dc/">
    <dcterms:isPartOf>foo</dcterms:isPartOf>
    <dcterms:title>bar</dcterms:title>
  </oai_dc:dc>
</collection>
        """
        s3 = boto3.client('s3', region_name='us-east-1')
        s3.put_object(Bucket=bucket, Key=key+'coll_1.xml', Body=coll_xml)
        s3.put_object(Bucket=bucket, Key=key+'coll_2.xml', Body=coll_xml)
        s3.put_object(Bucket=bucket, Key=key+'coll_3.xml', Body=coll_xml)


        with self.assertLogs(level='INFO') as log:
            field_count_report(bucket, key)

        stats = "\n\n"
        stats += "{http://purl.org/dc/terms/}isPartOf: |=========================|      3/3 | 100% \n"
        stats += "   {http://purl.org/dc/terms/}title: |=========================|      3/3 | 100% \n"
        stats += "\n"
        stats += "        dc_completeness 13.333333\n"
        stats += "collection_completeness 100.000000\n"
        stats += "      wwww_completeness 0.000000\n"
        stats += "   average_completeness 37.777778\n"
        self.assertEqual.__self__.maxDiff = None
        self.assertEqual(["INFO:root:" + stats], log.output)
