from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

import logging

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 tests=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.tables = tests
        self.redshift_conn_id = redshift_conn_id
        

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)
            self.log.info("Performing data quality checks")
        for test in self.tests:
            query = test.get("test")
            result = test.get("expected_result")
            record = redshift.get_first(query)
            if record is None or record[0] != result:
                raise ValueError(f"Data quality check failed. Running: {query}. Got: {record[0]}. Expected: {result}")
        
        
        self.log.info("Data quality checks finished")
