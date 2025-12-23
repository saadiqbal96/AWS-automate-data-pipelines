from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook


class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self, *args, **kwargs):
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        self.log.info('Starting StageToRedshiftOperator')

        params = self.params

        redshift_conn_id = params['redshift_conn_id']
        aws_credentials_id = params['aws_credentials_id']
        table = params['table']
        s3_bucket = params['s3_bucket']
        s3_key = params['s3_key']
        json_path = params['json_path']

        aws_hook = AwsHook(aws_credentials_id)
        credentials = aws_hook.get_credentials()

        redshift = PostgresHook(postgres_conn_id=redshift_conn_id)

        self.log.info(f'Clearing data from destination Redshift table {table}')
        redshift.run(f"DELETE FROM {table}")

        copy_sql = f"""
            COPY {table}
            FROM 's3://{s3_bucket}/{s3_key}'
            ACCESS_KEY_ID '{credentials.access_key}'
            SECRET_ACCESS_KEY '{credentials.secret_key}'
            REGION 'us-west-2'
            FORMAT AS JSON '{json_path}'
        """

        self.log.info(f'Executing COPY command for {table}')
        redshift.run(copy_sql)

        self.log.info('StageToRedshiftOperator completed')
