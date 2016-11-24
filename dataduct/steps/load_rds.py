"""
ETL step wrapper for SQLActivity to load data into Postgres
"""
from ..config import Config
from .etl_step import ETLStep
from ..pipeline import RdsNode
from ..pipeline import RdsDatabase
from ..utils.exceptions import ETLInputError
from ..pipeline import CopyActivity

config = Config()
if not hasattr(config, 'rds'):
    raise ETLInputError('Rds config not specified in ETL')

GLOBAL_RDS_CONFIG = config.rds


class LoadRdsStep(ETLStep):
    """Load Postgres Step class that helps load data into rds
    """

    def __init__(self,
                 table,
                 host_name,
                 insert_query,
                 max_errors=None,
                 replace_invalid_char=None,
                 **kwargs):
        """Constructor for the LoadPostgresStep class

        Args:
            table(path): table name for load
            sql(str): sql query to be executed
            rds_database(RdsDatabase): database to excute the query
            output_path(str): s3 path where sql output should be saved
            **kwargs(optional): Keyword arguments directly passed to base class
        """
        super(LoadRdsStep, self).__init__(**kwargs)

        rds_config = GLOBAL_RDS_CONFIG[host_name]

        region = rds_config['REGION']
        rds_instance_id = rds_config['RDS_INSTANCE_ID']
        user = rds_config['USERNAME']
        password = rds_config['PASSWORD']

        database_node = self.create_pipeline_object(
                    object_class=RdsDatabase,
                    region=region,
                    rds_instance_id=rds_instance_id,
                    username=user,
                    password=password,
        )

        # Create output node
        self._output = self.create_pipeline_object(
            object_class=RdsNode,
            schedule=self.schedule,
            database=database_node,
            table=table,
            username=user,
            password=password,
            select_query=None,
            insert_query=insert_query,
            host=rds_instance_id,
        )

        self.create_pipeline_object(
            object_class=CopyActivity,
            schedule=self.schedule,
            resource=self.resource,
            input_node=self.input,
            output_node=self.output,
            depends_on=self.depends_on,
            max_retries=self.max_retries,
        )

    @classmethod
    def arguments_processor(cls, etl, input_args):
        """Parse the step arguments according to the ETL pipeline

        Args:
            etl(ETLPipeline): Pipeline object containing resources and steps
            step_args(dict): Dictionary of the step arguments for the class
        """
        step_args = cls.base_arguments_processor(etl, input_args)
        step_args['rds_database'] = etl.rds_database

        return step_args
