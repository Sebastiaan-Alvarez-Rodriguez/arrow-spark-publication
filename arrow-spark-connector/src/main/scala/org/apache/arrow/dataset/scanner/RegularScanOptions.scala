package org.apache.arrow.dataset.scanner

import org.apache.arrow.util.filter.FilterUtil

object RegularScanOptions {
    def apply(columns: Array[String], batchSize: Long): RegularScanOptions = new RegularScanOptions(columns, batchSize)

    def apply(batchSize: Long): RegularScanOptions = apply(new Array[String](0), batchSize)

    def apply(): RegularScanOptions = apply(1)
}
class RegularScanOptions(columns: Array[String], batchSize: Long) extends ScanOptions(columns, FilterUtil.empty, batchSize)
