from lxml.etree import XMLPullParser, QName
from airflow.hooks.S3_hook import S3Hook
from funcake_dags.lib.metadata_breakers import dc_breaker
import logging

class Record(dc_breaker.Record):
    def __init__(self, elem):
        self.elem = elem

    def get_record_status(self):
        try:
            return self.elem.find("{*}header").get("status", "active")
        except:
            # Default for non oai-phm xml files
            return "new-updated"

    def get_elements(self):
        try:
            return self.elem[1][0]
        except:
            # Default for non oai-phm xml files
            return self.elem

    def get_stats(self):
        stats = {}
        for element in self.get_elements():
            fieldname = element.tag
            stats.setdefault(fieldname, 0)
            stats[fieldname] += 1
        return stats


class FieldCounter:
    def __init__(self):
        self.stats_aggregate={ "record_count": 0, "field_info": {} }
        self.s = 0

    def count(self, xml_string):
        parser = XMLPullParser(huge_tree=True)
        parser.feed(xml_string)
        tags = ["dc", "record"]
        for event, elem in parser.read_events():

            elem_tag = QName(elem.tag).localname
            if elem_tag in tags:
                if len(tags) == 2:
                    tags = [elem_tag]
                r = Record(elem)
                if (self.s % 1000) == 0 and self.s != 0:
                    logging.info("%d records processed" % self.s)
                self.s += 1
                if r.get_record_status() != "deleted":
                    dc_breaker.collect_stats(self.stats_aggregate, r.get_stats())
                elem.clear()
        parser.close()
        return self.stats_aggregate

    def report(self):
        stats_averages = dc_breaker.create_stats_averages(self.stats_aggregate)
        return dc_breaker.pretty_stats(stats_averages)


def field_count_report(bucket, source_prefix, conn_id="AIRFLOW_S3", **context):
    hook=S3Hook(conn_id)
    field_counter = FieldCounter()

    for key in hook.list_keys(bucket, source_prefix):
        response = hook.get_key(key, bucket).get()
        oai_xml = response['Body'].read()
        field_counter.count(oai_xml)

    if field_counter.stats_aggregate["record_count"] == 0:
        logging.warning("Nothing to report. No records were found.")
    else:
        logging.info(field_counter.report())
