from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import utils.functions as ut
import yaml
import os.path
from pyspark.sql.types import StructType, IntegerType, BooleanType,DoubleType

if __name__ == '__main__':
    current_dir = os.path.abspath(os.path.dirname(__file__))
    app_config_path = os.path.abspath(current_dir + "/../../" + "application.yml")
    app_secrets_path = os.path.abspath(current_dir + "/../../" + ".secrets")

    conf = open(app_config_path)
    app_conf = yaml.load(conf, Loader=yaml.FullLoader)
    secret = open(app_secrets_path)
    app_secret = yaml.load(secret, Loader=yaml.FullLoader)

    # Create the SparkSession
    spark = SparkSession \
        .builder \
        .appName("DataFrames examples") \
        .config("spark.mongodb.input.uri", app_secret["mongodb_config"]["uri"])\
        .getOrCreate()
    spark.sparkContext.setLogLevel('ERROR')

    # Setup spark to use s3
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.access.key", app_secret["s3_conf"]["access_key"])
    hadoop_conf.set("fs.s3a.secret.key", app_secret["s3_conf"]["secret_access_key"])

    src_list = app_conf['source_list']

    for src in src_list:
        src_conf = app_conf[src]

    cp_df = ut.read.from_s3(spark, src_conf)

    spark.sql("""
                SELECT
               DISTINCT REGIS_CNSM_ID, CAST(REGIS_CTY_CODE AS SMALLINT), CAST(REGIS_ID AS INTEGER),
               REGIS_LTY_ID, REGIS_DATE, REGIS_CHANNEL, REGIS_GENDER, REGIS_CITY, INS_DT
             FROM
               CP
             WHERE
             INS_DT = CURRENT_DATE
             """) \
        .show(5, False)