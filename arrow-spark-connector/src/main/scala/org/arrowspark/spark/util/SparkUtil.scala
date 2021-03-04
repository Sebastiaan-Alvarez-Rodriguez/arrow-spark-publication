package org.arrowspark.spark.util

import org.apache.spark.SparkConf

object SparkUtil {
    def getNumExecutors(sparkConf: SparkConf): Int = sparkConf.get("spark.executor.instances").toInt
}
