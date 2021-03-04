package org.apache.spark.sql.execution.datasources

import org.apache.spark.sql.{DataFrame, DataFrameReader}

package object arrow {
    /**
     * Implicitly adds an `arrow(path` function, allowing users to type `session.read.arrow(path)`
     * instead of `session.read.format("org.apache.arrow.spark.sql.execution.datasources.arrow")`.
     *
     * @example
     *
     * ' sparkSession.option(ArrowOptions.fileFormatProperty, "pq").arrow(path) ' // Note that Arrow will determine data format automatically if option is not set
     */
    implicit class ArrowDataframeReader(reader: DataFrameReader) {
        def arrow(path: String): DataFrame = reader.format("org.apache.spark.sql.execution.datasources.arrow.ArrowFileFormat").load(path)
    }
}
