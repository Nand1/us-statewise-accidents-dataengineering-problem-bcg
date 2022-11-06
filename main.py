import sys, getopt

import Analysis
import logging
from pyspark.sql.session import SparkSession
from py4j.java_gateway import java_import


def main():
    '''
    :param argv:
    :return: agruments
    '''
    spark = SparkSession.builder.appName("BCGUseCase").getOrCreate()
    #java_import(spark._jvm,"org.apache.spark.sql.api.python.*")
    log_manager = spark.sparkContext._jvm.org.apache.log4j
    logger = log_manager.LogManager.getLogger("MyLogger")
    logger.setLevel(log_manager.Level.DEBUG)
    logger.info("pyspark script logger initialized")
    inputLocation = sys.argv[1]
    outputLocation= sys.argv[2]
    logger.info(inputLocation)
    logger.info(outputLocation)
    Analysis.Analysis_crash_data().analysis_function(inputLocation, outputLocation,spark,logger).__doc__
if __name__ == "__main__":

    main().__doc__

    #

