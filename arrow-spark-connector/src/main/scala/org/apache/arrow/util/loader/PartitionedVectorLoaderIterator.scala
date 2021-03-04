package org.apache.arrow.util.loader

import org.apache.arrow.dataset.scanner.ScanTask
import org.apache.arrow.vector.{FieldVector, VectorSchemaRoot}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.vectorized.ColumnVectorUtils
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.{ColumnVector, ColumnarBatch}
import org.arrowspark.arrow.dataset.vector.ArrowWritableColumnVector

import scala.collection.JavaConverters._

/** Companion object to [[PartitionedVectorLoaderIterator]] */
object PartitionedVectorLoaderIterator {
    def apply(delegate: Iterator[ScanTask.ArrowBundledVectors], partitionValues: InternalRow, partitionSchema: StructType, dataSchema: StructType, allowOffheap: Boolean=false): PartitionedVectorLoaderIterator =
        new PartitionedVectorLoaderIterator(delegate, partitionValues, partitionSchema, dataSchema, allowOffheap)
}

class PartitionedVectorLoaderIterator(delegate: Iterator[ScanTask.ArrowBundledVectors], partitionValues: InternalRow, partitionSchema: StructType, dataSchema: StructType, allowOffheap: Boolean=false) extends VectorLoaderIterator(delegate, dataSchema, allowOffheap) {
    override def next(): ColumnarBatch = {
        val abv: ScanTask.ArrowBundledVectors = delegate.next()
        val valueVectors: VectorSchemaRoot = abv.valueVectors

        val loadedVectors: List[FieldVector] = loadData(valueVectors)
        val dictionaryVectors: List[FieldVector] = loadDict(valueVectors, abv.dictionaryVectors, dataSchema)

        val rowCount: Int = valueVectors.getRowCount

        val loadedColumns: Array[ArrowWritableColumnVector] = ArrowWritableColumnVector.loadColumns(rowCount, loadedVectors.asJava, dictionaryVectors.asJava, allowOffheap)
        val partitionColumns: Array[ArrowWritableColumnVector] = ArrowWritableColumnVector.allocateColumns(rowCount, partitionSchema, allowOffheap)

        partitionColumns.indices.foreach(i => {
            ColumnVectorUtils.populate(partitionColumns(i), partitionValues, i)
            partitionColumns(i).setIsConstant()
        })

        new ColumnarBatch((loadedColumns ++ partitionColumns).asInstanceOf[Array[ColumnVector]], rowCount)
    }
}