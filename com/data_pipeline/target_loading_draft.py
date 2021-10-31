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

    tgt_list = app_conf['target_list']

    for tgt in tgt_list:
        tgt_conf = tgt_conf[tgt]

        if tgt == 'REGIS_DIM':
            src_list = tgt_conf['source_data']
            for src in src_list:
                file_path = "s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/" + app_conf["s3_conf"]["staging_dir"] + "/" + src
                src_df = spark.sql("select * from parquet.'{}'".format(file_path))
                src_df.printSchema()
                src_df.show(5, False)
                src_df.createOrReplaceTempView(src)

            print("REGIS_DIM")

            regis_dim = spark.sql(app_conf["REGIS_DIM"]["loadingQuery"])
            regis_dim.show(5, False)

            regis_dim.write_to_redshist(regis_dim.coalesce(1),
                                        app_secret,
                                        "s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/temp",
                                        tgt_conf['tableName'])

        elif tgt == 'CHILD_DIM':
            print('CHILD_DIM')


