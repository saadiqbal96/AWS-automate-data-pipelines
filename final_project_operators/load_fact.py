from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadFactOperator(BaseOperator):
    ui_color = '#F98866'

    @apply_defaults
    def __init__(self, *args, **kwargs):
        super(LoadFactOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        self.log.info('Starting LoadFactOperator')

        params = self.params
        redshift_conn_id = params['redshift_conn_id']
        table = params['table']
        sql = params['sql']

        redshift = PostgresHook(postgres_conn_id=redshift_conn_id)

        insert_sql = f"""
            INSERT INTO {table}
            {sql}
        """

        self.log.info(f'Loading fact table {table}')
        redshift.run(insert_sql)

        self.log.info('LoadFactOperator completed')
