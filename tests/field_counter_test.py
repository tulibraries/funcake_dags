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



    @mock_s3
    def test_field_count_report_with_no_records(self):
        bucket = "tulib-airflow-prod"
        key = "foobar/2019-12-12_17-17-40/transformed-filtered/"
        conn = boto3.resource('s3', region_name='us-east-1')
        conn.create_bucket(Bucket=bucket)

        coll_xml="<collection></collection>"

        s3 = boto3.client('s3', region_name='us-east-1')
        s3.put_object(Bucket=bucket, Key=key+'coll_1.xml', Body=coll_xml)
        s3.put_object(Bucket=bucket, Key=key+'coll_2.xml', Body=coll_xml)
        s3.put_object(Bucket=bucket, Key=key+'coll_3.xml', Body=coll_xml)


        with self.assertLogs(level='INFO') as log:
            field_count_report(bucket, key)

        stats = "Nothing to report. No records were found."
        self.assertEqual.__self__.maxDiff = None
        self.assertEqual(["WARNING:root:" + stats], log.output)


    @mock_s3
    def test_field_count_report_with_record_tag(self):
        bucket = "tulib-airflow-prod"
        key = "foobar/2019-12-12_17-17-40/transformed-filtered/"
        conn = boto3.resource('s3', region_name='us-east-1')
        conn.create_bucket(Bucket=bucket)

        coll_xml="""
<oai:collection xmlns:oai="http://www.openarchives.org/OAI/2.0/" dag-id="funcake_lcp" dag-timestamp="None"><oai:record airflow-record-id="oai:digital.librarycompany.org:digitool_118292"><oai:header><oai:identifier>oai:digital.librarycompany.org:digitool_118292</oai:identifier><oai:datestamp>2018-09-17T08:02:08Z</oai:datestamp><oai:setSpec>Islandora_EPHEM</oai:setSpec><oai:setSpec>Islandora_HELF3</oai:setSpec></oai:header><oai:metadata><oai_dc:dc xmlns:srw_dc="info:srw/schema/1/dc-schema" xmlns:dc="http://purl.org/dc/elements/1.1/" xmlns:oai_dc="http://www.openarchives.org/OAI/2.0/oai_dc/" xmlns:dcterms="http://purl.org/dc/terms/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.openarchives.org/OAI/2.0/oai_dc/ http://www.openarchives.org/OAI/2.0/oai_dc.xsd"><dc:title>[William H. Helfand graphic popular medicine stationery collection]</dc:title><dc:contributor>Dunston, G. H. (George H.)</dc:contributor><dc:contributor>Weber, Edward (Edward), d. 1848</dc:contributor><dc:contributor>A. Hoen &amp; Co.</dc:contributor><dc:contributor>American Bank Note Company</dc:contributor><dc:contributor>Calvert Lithographing Co. (Detroit, Mich.)</dc:contributor><dc:contributor>Craig, Butt &amp; Finley</dc:contributor><dc:contributor>Craig, Finley &amp; Co.</dc:contributor><dc:contributor>Detroit Lithographing Company</dc:contributor><dc:contributor>Ketterlinus Printing House</dc:contributor><dc:contributor>Gast Banknote &amp; Lithographing Co.</dc:contributor><dc:contributor>Gies &amp; Co.</dc:contributor><dc:contributor>Longacre &amp; Co.</dc:contributor><dc:contributor>Major &amp; Knapp Engraving, Manufacturing &amp; Lithographic Co.</dc:contributor><dc:contributor>Photo-Electroype Engraving Co.</dc:contributor><dc:contributor>Photo Engraving Co.</dc:contributor><dc:contributor>Shober &amp; Carqueville</dc:contributor><dc:contributor>Smith Bros. (Philadelphia, Pa.)</dc:contributor><dc:contributor>Snyder &amp; Black Lithogrs</dc:contributor><dc:contributor>Strobridge &amp; Co. Lith</dc:contributor><dc:contributor>Wm. H. Brett &amp; Co.</dc:contributor><dc:type>StillImage</dc:type><dc:subject>Billheads -- 1840-1940</dc:subject><dc:subject>Envelopes -- 1840-1940</dc:subject><dc:subject>Form letters -- 1860-1900</dc:subject><dc:subject>Engravings -- 1840-1900</dc:subject><dc:subject>Letterheads -- 1840-1940</dc:subject><dc:subject>Lithographs -- 1840-1940</dc:subject><dc:subject>Photomechanical prints -- 1880-1940</dc:subject><dc:subject>Song sheets -- 1880-1940</dc:subject><dc:subject>Stereographs -- 1880-1940</dc:subject><dc:date>[ca. 1840-1935, bulk 1870-1890]</dc:date><dc:language>eng</dc:language><dcterms:extent>490 items: lithographs, relief prints, photomechanical prints, engravings, and albumen; 36 x 21 cm.(8.5 x 14.5 in.) or smaller.</dcterms:extent><dc:subject>Aspinwall, James S.</dc:subject><dc:subject>Blanding, William B., 1826-1892</dc:subject><dc:subject>Brunswig, L. N. (Lucien Napoelon), 1854-1943</dc:subject><dc:subject>Hamilton, William, A., 1828-1900</dc:subject><dc:subject>Knowlson, A. M.</dc:subject><dc:subject>Orvis, P. D.</dc:subject><dc:subject>Van Duzer, Selah Reeve, 1823-1903</dc:subject><dc:subject>A.W. Wright &amp; Co.</dc:subject><dc:subject>Barker, Moore &amp; Mein</dc:subject><dc:subject>Bean &amp; Stevenson</dc:subject><dc:subject>Browning &amp; Brothers</dc:subject><dc:subject>Caswell, Massey &amp; Co.</dc:subject><dc:subject>C.H. Butterworth &amp; Co.</dc:subject><dc:subject>C.J. Lincoln Company (Little Rock, Ark.)</dc:subject><dc:subject>College of Pharamcy of the City of New York</dc:subject><dc:subject>Geo. C. Goodwin &amp; Co.</dc:subject><dc:subject>Hall &amp; Ruckel</dc:subject><dc:subject>Hopkins-Weller Drug Co.</dc:subject><dc:subject>James Baily &amp; Son</dc:subject><dc:subject>J.D. Marshall &amp; Bros.</dc:subject><dc:subject>J.L. Lyons &amp; Co.</dc:subject><dc:subject>Lanman &amp; Kemp</dc:subject><dc:subject>McKesson &amp; Robbins, inc</dc:subject><dc:subject>Nichols &amp; Harris (New London, Conn.)</dc:subject><dc:subject>Robert Shoemaker &amp; Co.</dc:subject><dc:subject>Strother Drug Co.</dc:subject><dc:subject>Wells, Richardson &amp; Co.</dc:subject><dc:subject>W.H. Schieffelin &amp; Co. (New York, N.Y.)</dc:subject><dc:subject>W.J. Gilmore &amp; Co.</dc:subject><dc:subject>Decoration and ornamentation</dc:subject><dc:subject>Decoration and ornamentation -- Art nouveau</dc:subject><dc:subject>Dental offices</dc:subject><dc:subject>Drugstores</dc:subject><dc:subject>Factories -- United States</dc:subject><dc:subject>Medical supply industry -- United States</dc:subject><dc:subject>Mortars &amp; pestles</dc:subject><dc:subject>Pharmaceutical industry -- New York (State)</dc:subject><dc:subject>Pharmaceutical industry -- New York (State) -- New York</dc:subject><dc:subject>Pharmaceutical industry -- Pennsylvania</dc:subject><dc:subject>Pharmaceutical industry -- Pennsylvania -- Philadelphia</dc:subject><dc:subject>Pharmaceutical industry -- United States</dc:subject><dc:subject>Pharmacists</dc:subject><dc:subject>Phoenix</dc:subject><dc:subject>Storefronts -- United States</dc:subject><dc:description>Collection of stationery, primarily illustrated and typographical letterheads, billheads, and form letters, of pharmaceutical firms and related businesses and institutions in the United States (predominantly New York City, New York, Philadelphia, and Pennsylvania) issued between circa 1840 and 1935. Subjects include invoices and receipts, shipping arrangements and fees, product orders, payments and payment disputes. Firms, businesses, and institutions well represented include James S. Aspinwall; William P. Blanding; L. N. Brunswig; Caswell, Massey &amp; Co.; C.J. Lincoln Co.; College of Pharmacy of the City of New York; Geo. C. Goodwin &amp; Co.; Hall &amp; Ruckel; Dr. William A. Hammond's Sanitarium; Hopkins-Weller Drug Co.; James Baily &amp; Son; J. D. Marshall &amp; Bros. (D. Marshall &amp; Bro.); J. L. Lyons &amp; Co.; A. M. Knowlson; Lanman &amp; Kemp; McKesson &amp; Robbins; Nichols &amp; Harris; P. D. Orvis; S. R. Van Duzer; Wells, Richardson &amp; Co. (Wells &amp; Richardson Co.); Wilson Drug Co.; and W. J. Gilmore &amp; Co. Philadelphia firms represented include A.W. Wright &amp; Co.; Barker, Moore, Mein; Bean &amp; Stevenson; Browning &amp; Brothers; C.H. Butterworth &amp; Co.; Robert Shoemaker &amp; Co.; W.H. Schieffelin &amp; Co.; and Strother Drug Co. Collection also contains several pieces of stationery of firms in New England, including Massachussetts (particularly Boston), Maine, Connecticut, Vermont, and Rhode Island; the Mid-West, including Ohio, Michigan, and Minnesota; and the South, including Louisiana (particularly New Orleans), Virginia, and Tennessee. A small number of items also represent businesses in the Western United States, United Kingdom, and Canada. A song sheet, envelope, and stereograph also form the collection.; Illustrations depict various subjects. The most numerous are views of pharmaceutical factories and storefronts, often including street and pedestrian traffic. Imagery also depicts pharmaceutical apparatus and trademarks, including mortars and pestles; medical supplies, including trusses; allegorical scenes; heraldry; and art nouveau pictorial details and designs. Other illustrations show medieval apothecaries; the interiors of a pharmacy and dental office; and the mythical creature phoenix.; Title supplied by cataloger.; Various engravers, printers, and publishers, including Smith Bros.; Gast; A. Hoen &amp; Co.; Collier &amp; Cleveland; Craig, Butt, &amp; Finley; Calvert Lithographing Co.; Snyder &amp; Black; Detroit Litho. Co.; American Bank Note Company; Gies &amp; Co.; Strobridge &amp; Co.; E. Weber; Craig, Finley &amp; Co.; G. H. Dunston; and Wm. H. Brett &amp; Co.; Majority of the billheads, letterheads, and form letters completed in manuscript or type and contain manuscript and typewritten notes on recto and verso.</dc:description><dc:rights>http://rightsstatements.org/vocab/NoC-US/1.0/</dc:rights><dcterms:hasFormat>https://digital.librarycompany.org/islandora/object/digitool%3A118292/datastream/TN/view/%5BWilliam%20H.%20Helfand%20graphic%20popular%20medicine%20stationery%20collection%5D.jpg</dcterms:hasFormat><dc:identifier>https://digital.librarycompany.org/islandora/object/digitool%3A118292</dc:identifier></oai_dc:dc></oai:metadata></oai:record></oai:collection>
        """
        s3 = boto3.client('s3', region_name='us-east-1')
        s3.put_object(Bucket=bucket, Key=key+'coll_1.xml', Body=coll_xml)
        s3.put_object(Bucket=bucket, Key=key+'coll_2.xml', Body=coll_xml)
        s3.put_object(Bucket=bucket, Key=key+'coll_3.xml', Body=coll_xml)


        with self.assertLogs(level='INFO') as log:
            field_count_report(bucket, key)

        self.assertIn("3/3", log.output[0])
