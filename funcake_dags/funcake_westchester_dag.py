from funcake_dags.template import create_dag


DAG = create_dag(
        dag_id="foo",
        oai_config_name="WESTCHESTER_OAI_CONFIG",
        xsl_config_name="WESTCHESTER_XSL_CONFIG",
        target_alias_env_name="WESTCHESTER_TARGET_ALIAS_ENV")

# Example OAI Harvest Config Variable
# {
#   "endpoint": "http://digital.klnpa.org/oai/oai.php",
#   "md_prefix": "oai_qdc",
#   "all_sets": "False", <--- OPTIONAL
#   "excluded_sets": [], <--- OPTIONAL
#   "included_sets": ["aebye","ajt","amc", ...], <--- OPTIONAL
#   "schematron_filter": "validations/dcingest_reqd_fields.sch",
#   "schematron_report": "validations/padigital_missing_thumbnailURL.sch"
# }


# Example XSL Config Variable
#{
#   "schematron_filter": "validations/funcake_reqd_fields.sch",
#   "schematron_report": "validations/padigital_missing_thumbnailURL.sch",
#   "xsl_branch": "master", <--- OPTIONAL
#   "xsl_filename": "transforms/dplah.xsl",
#   "xsl_repository": "tulibraries/aggregator_mdx" <--- OPTIONAL
# }
