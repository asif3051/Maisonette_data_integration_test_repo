from Maisonette.ParameterRead import get_parameter_values
from awsglue.utils import getResolvedOptions


# def rdsConnectionInfo():
        
#     retrieved_values, rdsTables = get_parameter_values()

#     return retrieved_values, rdsTables


#extract function
def extract(spark):

    
    # retrieved_values = rdsConnectionInfo()
    retrieved_values, rdsTables = get_parameter_values()

    rawdf = spark.read.format("jdbc")\
                  .option("url", "jdbc:postgresql://{rds_host}:{rds_port}/{rds_dbname}".format(rds_host=retrieved_values["DB_HOST"], rds_port=retrieved_values["DB_PORT"], rds_dbname=retrieved_values["DB_NAME"]))\
                  .option("dbtable", rdsTables["source_table"]) \
                  .option("user", retrieved_values["DB_USERNAME"]) \
                  .option("password", retrieved_values["DB_PASSWORD"]) \
                  .option("driver","org.postgresql.Driver")\
                  .load()

    return rawdf
