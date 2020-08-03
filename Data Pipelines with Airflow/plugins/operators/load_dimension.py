from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers import SqlQueries

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 table_name,
                 redshift_conn_id="",
                 append_data=False,
                 sql_statement="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.append_data = append_data
        self.sql_statement = sql_statement
        self.table_name = table_name

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        if self.append_data == False:
            sql_statement = 'DELETE FROM %s' % self.table_name
            redshift.run(sql_statement)
            
        sql_statement = 'INSERT INTO %s %s' % (self.table_name, self.sql_statement)
        redshift.run(sql_statement)
        
        self.log.info('The dimension tables have been loaded successfully!')
