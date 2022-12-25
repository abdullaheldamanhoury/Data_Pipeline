from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook


class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    template_fields = ('s3_key',)
    
    copy_query="""
    COPY {} 
    FROM '{}'  
    region 'us-west-2' 
    ACCESS_KEY_ID '{}' 
    SECRET_ACCESS_KEY '{}' 
    TIMEFORMAT as 'epochmillisecs' 
    TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL 
    FORMAT AS JSON '{}' 
    COMPUPDATE OFF
    """


    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="", 
                 s3_key="",
                 LOG_JSONPATH="",
                 drop_table="",
                 create_table="",
                 *args, 
                 **kwargs):
        

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.LOG_JSONPATH = LOG_JSONPATH
        self.aws_credentials_id = aws_credentials_id

    def execute(self, context):
        
        """The data from S3 is copied to Redshift staging tables by StageToRedshiftOperator 
        which is able to load any JSON format files from S3 to Amazon Redshift.
        parameters:
            - redshift conn_id :This is the connection details between Airflow and
            the data warehouse in Amazon Redshift.
            - aws_credentials_id: this is the credentials to connect between Airflow and S3 bucket.
            - LOG_JSONPATH : this is the JSON manifest file of log_data.
            - table: this is the table where the data from S3 is to be copied.
            - s3_bucket: this is the S3 bucket where the table resides.
            - s3_key: this is the table directory inside S3 bucket. """
        
        # Connect to the AWS
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        # Connect to the redshift
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        # Clearing table from data before copying data to it
        self.log.info(f"Clearing {self.table} table from data")
        redshift.run("DELETE FROM {}".format(self.table))
        self.log.info(f"Data of {self.table} table is successfully deleted")
        
        # Copying data from S3 to Redshift
        self.log.info("Copying data from S3 to Redshift")
        rendrd_key = self.s3_key.format(**context)
        s3_p = "s3://{}/{}".format(self.s3_bucket, rendrd_key)
        
        redshift.run(StageToRedshiftOperator.copy_query.format(
                self.table,
                s3_p,
                credentials.access_key,
                credentials.secret_key,
                self.LOG_JSONPATH ))
        self.log.info(f' Copying data from S3 to Redshift is done successfully')
