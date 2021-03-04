package org.apache.spark.sql.util

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType

object ArrowFileFormatHelper {
    def supportBatch(session: SparkSession, schema: StructType): Boolean = {
        val conf = session.sessionState.conf
        conf.parquetVectorizedReaderEnabled && conf.wholeStageEnabled &&
            schema.length <= conf.wholeStageMaxNumFields &&
            schema.forall(_.dataType.isInstanceOf[org.apache.spark.sql.types.AtomicType])
    }
}
