import boto3
import json
import sys
from awsglue.utils import getResolvedOptions

def get_parameter_values():
    
    parameter_name_list = ['/qa/syndication/DB_NAME','/qa/syndication/DB_PASSWORD','/qa/syndication/DB_PORT','/qa/syndication/DB_USERNAME','/qa/syndication/DB_HOST']
    retrieved_values = {}
    
    ssm_client = boto3.client('ssm')

    for parameter_name in parameter_name_list:
        print(parameter_name)
        response = ssm_client.get_parameter(Name=parameter_name, WithDecryption=True)
        parameter_values = response['Parameter']['Value']
        retrieved_values[parameter_name[16::]] = parameter_values
        print(retrieved_values)


    args = getResolvedOptions(sys.argv, ['SOURCE_TABLE', 'TARGET_TABLE'])
    rdsTables = {
        "source_table" : args['SOURCE_TABLE'],
        "target_table" : args['TARGET_TABLE']
    }


    # print("source table : {source_table}".format(source_table = rdsTables["source_table"]))
    # print("target table : {target_table}".format(target_table = rdsTables["target_table"]))


    return retrieved_values, rdsTables
    