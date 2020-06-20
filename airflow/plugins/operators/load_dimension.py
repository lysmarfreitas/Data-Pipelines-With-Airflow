from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    ui_color = '#80BD9E'

    insert_sql = """
        INSERT INTO {}
        {};
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 delete_load=False,
                 load_sql_stmt="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.delete_load = delete_load
        self.load_sql_stmt = load_sql_stmt
        

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        if self.delete_load:
            self.log.info("Clearing data from destination Redshift table")
            redshift.run("Deleting from {}".format(self.table))

        formatted_sql = LoadDimensionOperator.insert_sql.format(
            self.table,
            self.load_sql_stmt
        )
        self.log.info(f"Executing {formatted_sql} ...")
        redshift.run(formatted_sql)
