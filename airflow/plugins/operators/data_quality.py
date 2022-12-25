from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 data_quality_checks=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.data_quality_checks = data_quality_checks

    def execute(self, context):
        
        
        """
        This operator is used to check the quality of data through 
        comparing the test data and real data with each other and if they are identical or not.
        If the test result is greater than zero, then the test result is not fault until it accumulates 
        the data up to the target day which is 30-11-2018 and if the test data and real data are
        identical then the quality check is passed. If any table results zero count, 
        this will consider quality check is failed.
        parameters:
            - redshift conn_id :This is the connection details between Airflow and 
            the data warehouse in Amazon Redshift.
            - tables : List containing dictionary for each table with query to get 
            the test result and the real result to compare between of them.
            """
        # Connect to the redshift
        redshift = PostgresHook(self.redshift_conn_id)
        count_error  = 0
        
        for check in self.data_quality_checks:
            query_check = check.get('data_sql')
            real_result = check.get('real_value')
            test_result = redshift.get_records(query_check)[0][0]
            
            self.log.info(f"The query is : {query_check}")
            self.log.info(f"The expected result is : {real_result}")
            self.log.info(f"The Check result is : {test_result}")
            if test_result > 0:
                if test_result != real_result:
                    self.log.info(f"Data quality check pass but doesn't count up to the day 30 yet ")
                if test_result == real_result:
                    self.log.info(f"Data quality check pass and reach to count the day 30 ")
            else:
                
                count_error += 1
                self.log.info(f"Data quality check fails at :{query_check}")
        if count_error > 0:
            
            self.log.info('Data quality checks failed')
        else:
            self.log.info('Data quality checks passed')