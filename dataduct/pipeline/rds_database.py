"""
Pipeline object class for Rds database
"""

from .pipeline_object import PipelineObject

class RdsDatabase(PipelineObject):
    """RdsDatabase resource class
    """

    def __init__(self,
                 id,
                 region,
                 rds_instance_id,
                 database,
                 username,
                 password):
        """Constructor for the RdsDatabase class

        Args:
            id(str): id of the object
            region(str): code for the region where the database exists
            rds_instance_id(str): identifier of the DB instance
            username(str): username for the database
            password(str): password for the database
        """

        kwargs = {
            'id': id,
            'type': 'RdsDatabase',
            'region': region,
            'rdsInstanceId': rds_instance_id,
            'databaseName': database,
            'username': username,
            '*password': password,
        }
        super(RdsDatabase, self).__init__(**kwargs)
