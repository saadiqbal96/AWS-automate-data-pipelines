from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadDimensionOperator(BaseOperator):
    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self, *args, **kwargs):
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        self.log.info('Starting LoadDimensionOperator')

        params = self.params
        redshift_conn_id = params['redshift_conn_id']
        table = params['table']
        sql = params['sql']
        append_only = params.get('append_only', False)

        redshift = PostgresHook(postgres_conn_id=redshift_conn_id)

        if not append_only:
            self.log.info(f'Clearing data from dimension table {table}')
            redshift.run(f'DELETE FROM {table}')

        insert_sql = f"""
            INSERT INTO {table}
            {sql}
        """

        self.log.info(f'Loading dimension table {table}')
        redshift.run(insert_sql)

        self.log.info('LoadDimensionOperator completed')
