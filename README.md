### Project Overview

- This project will introduce concepts of Apache Airflow. I create my own custom operators to perform tasks such as staging the data, filling the data warehouse, and running checks on the data as the final step.

### Datasets

1. Song Dataset: a subset of real data from the Million Song Dataset. Each file is in JSON format and contains metadata about a song and the artist of that song. The files are partitioned by the first three letters of each song's track ID.For example, here are file paths to two files in this dataset:
    - song_data/A/B/C/TRABCEI128F424C983.json
    - song_data/A/A/B/TRAABJL12903CDCF1A.json

2. Log Dataset: consists of log files in JSON format generated by this event simulator based on the songs in the dataset above. These simulate app activity logs from an imaginary music streaming app based on configuration settings.The log files in the dataset you'll be working with are partitioned by year and month. For example, here are file paths to two files in this dataset:
    - log_data/2018/11/2018-11-12-events.json
    - log_data/2018/11/2018-11-13-events.json


### components for the project
1. The dag template has all the imports and task templates in place, but the task dependencies have not been set.
2. The operators folder with operator templates.
3. A helper class for the SQL drops, creates tables and transformations.
4. dwh.cfg Configure Redshift cluster and data import
5. dw_test.ipynb to Create IAM role, Redshift cluster, and allow TCP connection

### Building the operators

#### I build four different operators that will stage the data, transform the data, and run checks on data quality.

1. Stage Operator:
    - The stage operator is expected to be able to load any JSON formatted files from S3 to Amazon Redshift. The operator creates and runs a SQL COPY statement based on the parameters provided. The operator's parameters should specify where in S3 the file is loaded and what is the target table.

    - The parameters should be used to distinguish between JSON file. Another important requirement of the stage operator is containing a templated field that allows it to load timestamped files from S3 based on the execution time and run backfills.

2. Fact and Dimension Operators:
    - With dimension and fact operators, I can utilize the provided SQL helper class to run data transformations. Most of the logic is within the SQL transformations and the operator is expected to take as input a SQL statement and target database on which to run the query against. I can also define a target table that will contain the results of the transformation.

    - Dimension loads are often done with the truncate-insert pattern where the target table is emptied before the load. Thus, you could also have a parameter that allows switching between insert modes when loading dimensions. Fact tables are usually so massive that they should only allow append type functionality.

3. Data Quality Operator:
    - The final operator to create is the data quality operator, which is used to run checks on the data itself. The operator's main functionality is to receive one or more SQL based test cases along with the expected results and execute the tests. For each the test, the test result and expected result needs to be checked and if there is no match, the operator should raise an exception and the task should retry and fail eventually.

### Project Steps

1. In AWS account, create a new IAM user so that environment variables AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY could be set.
2. Set AdministratorAccess with the IAM user
3. Use variables AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY to create  S3, EC2, and Redshift
4. Choose DB/DB_PASSWORD in dhw.cfg.
5. Create IAM role, Redshift cluster, and configure TCP connectivity
6. Write SQL create statements to create tables in create_tables.sql file and insert statements in sql_queries file.
7. create operators
8. Run dw_test.ipynb
9. Fill dwh.cfg file with the outputs from dw_test.ipynb (HOST and ARN) to create redshift cluster.
10. I used Airflow's UI to configure your AWS credentials.

    - To go to the Airflow UI:

        - In a classroom workspace, I run the script /opt/airflow/start.sh. Once I see the message "Airflow web server is ready" click on the blue Access Airflow button in the bottom right.
Click on the Admin tab and select Connections

11. On the create connection page, enter the following values:
    - Conn Id: Enter aws_credentials.
    - Conn Type: Enter Amazon Web Services.
    - Login: Enter Access key ID from the IAM User credentials.
    - Password: Enter Secret access key from the IAM User credentials.

12. Add Airflow Connections to AWS Redshift:
    - Conn Id: Enter redshift.
    - Conn Type: Enter Postgres.
    - Host: Enter the endpoint of my Redshift cluster
    - Schema: Enter dwh. 
    - Login: Enter dwhuser.
    - Password: Enter the password I created when launching myRedshift cluster.
    - Port: Enter 5439. 
    - select Save.

13. turn on Dag in airflow server
 









