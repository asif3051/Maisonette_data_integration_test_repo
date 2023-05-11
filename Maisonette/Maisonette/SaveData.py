from awsglue.utils import getResolvedOptions
from Maisonette.ParameterRead import get_parameter_values


# def rdsConnectionInfo():
        
#     retrieved_values = get_parameter_values()

#     return retrieved_values


def load_data_rds(df):
    
    # retrieved_values = rdsConnectionInfo()
    retrieved_values, rdsTables = get_parameter_values()

    df.write \
            .format("jdbc")\
            .mode("append")\
            .option("url", "jdbc:postgresql://{rds_host}:{rds_port}/{rds_dbname}".format(rds_host=retrieved_values["DB_HOST"], rds_port=retrieved_values["DB_PORT"], rds_dbname=retrieved_values["DB_NAME"]))\
            .option("dbtable", rdsTables["target_table"]) \
            .option("user", retrieved_values["DB_USERNAME"]) \
            .option("password", retrieved_values["DB_PASSWORD"]) \
            .option("driver", "org.postgresql.Driver") \
            .option("mode", "append") \
            .save()
    
