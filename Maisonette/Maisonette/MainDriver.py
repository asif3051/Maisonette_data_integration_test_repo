import sys
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from Maisonette.ParameterRead import get_parameter_values
from Maisonette.ReadData import extract
from Maisonette.Flatten import flatten
from Maisonette.ProcessName import process_name
from Maisonette.SaveData import load_data_rds



def main():
    spark = SparkSession \
                    .builder \
                    .appName("Maisonette") \
                    .getOrCreate()
    
    #extract
    rawdf = extract(spark)


    #tranform
    retrundf = flatten(spark,rawdf)
    retrundf_processed = process_name(retrundf)


    #load
    load_data_rds(retrundf_processed)
    
    
    spark.stop()
   

    