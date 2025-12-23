from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self, *args, **kwargs):
        super(DataQualityOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        self.log.info('Starting DataQualityOperator')

        params = self.params
        redshift_conn_id = params['redshift_conn_id']
        tests = params.get('tests', [])

        if not tests:
            raise ValueError('No data quality tests provided')

        redshift = PostgresHook(postgres_conn_id=redshift_conn_id)

        for test in tests:
            sql = test.get('sql')
            expected = test.get('expected_result')

            records = redshift.get_records(sql)

            if not records or not records[0]:
                raise ValueError(f'Data quality check failed. No results for: {sql}')

            actual = records[0][0]

            if actual != expected:
                raise ValueError(
                    f'Data quality check failed. '
                    f'Expected {expected}, got {actual}. '
                    f'SQL: {sql}'
                )

            self.log.info(f'Data quality check passed: {sql}')

        self.log.info('DataQualityOperator completed successfully')
