from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    table_insert = """INSERT INTO {table} {insert_tbl};"""


    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 insert_tbl="",
                 drop_table="",
                 create_table="",
                 truncate="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.insert_tbl = insert_tbl
        self.truncate = truncate

    def execute(self, context):
        
        """
        In this phase, the dimension tables are loaded from the staging tables by LoadDimensionOperator.
        parameter:
            - redshift conn_id :This is the connection details between Airflow and 
            the data warehouse in Amazon Redshift.
            - table : the destination of dimension table.
            - insert_tbl: the SQL query to insert the data in the destination table.
            - truncate : clear data from table and then insert the values in the dimension table
            """
        # Connect to the redshift
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        # Clearing table from data before inserting data to it
        if self.truncate:
            self.log.info(f"Clearing {self.table} table from data")
            redshift.run("DELETE FROM {}".format(self.table))
            self.log.info(f"Data of {self.table} table is successfully deleted")
        # Data insertion
        self.log.info(f'Load table {self.table}')
        redshift.run(LoadDimensionOperator.table_insert.format(table = self.table,insert_tbl = self.insert_tbl))
        self.log.info(f' {self.table} table loaded successfully')
