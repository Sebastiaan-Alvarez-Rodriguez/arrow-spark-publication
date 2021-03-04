package org.apache.arrow.util.loader

import org.apache.arrow.dataset.scanner.ScanTask
import org.apache.arrow.vector.dictionary.Dictionary
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch
import org.apache.arrow.vector.types.pojo.ArrowType.ArrowTypeID
import org.apache.arrow.vector.{FieldVector, VectorSchemaRoot}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.{ColumnVector, ColumnarBatch}
import org.arrowspark.arrow.dataset.vector.ArrowWritableColumnVector

import scala.collection.JavaConverters.{asScalaBufferConverter, seqAsJavaListConverter}


/**
 * Iterator consuming [[ArrowRecordBatch]] Arrow IPC messages, producing [[ColumnarBatch]] objects
 * @param delegate Iterator providing interpreted Arrow IPC messages (including a loaded [[VectorSchemaRoot]])
 * @param dataSchema Schema of the data we will read
 * @param allowOffheap Whether we are allowed to use intermediate off-heap memory. This is slightly faster than on-heap memory, as we don't have to deal
 *                     with JVM objects in raw off-heap memory.
 *                     '''Warning:''' Only set this option if [[org.apache.spark.sql.util.ArrowOptions.memoryMode]] is set to [[org.apache.spark.memory.MemoryMode.OFF_HEAP]].
 *                     Otherwise, Spark will crash when we try to make an off-heap allocation
 */
class VectorLoaderIterator(protected val delegate: Iterator[ScanTask.ArrowBundledVectors], dataSchema: StructType, allowOffheap: Boolean=false) extends Iterator[ColumnarBatch] {

    override def hasNext: Boolean = delegate.hasNext

    override def next(): ColumnarBatch = {
        val abv: ScanTask.ArrowBundledVectors = delegate.next()
        val valueVectors: VectorSchemaRoot = abv.valueVectors

        val loadedVectors: List[FieldVector] = loadData(valueVectors)
        val dictionaryVectors: List[FieldVector] = loadDict(valueVectors, abv.dictionaryVectors, dataSchema)

        val rowCount: Int = valueVectors.getRowCount

        val loadedColumns: Array[ArrowWritableColumnVector] = ArrowWritableColumnVector.loadColumns(rowCount, loadedVectors.asJava, dictionaryVectors.asJava, allowOffheap)
        new ColumnarBatch(loadedColumns.asInstanceOf[Array[ColumnVector]], rowCount)
    }

    protected def loadData(valueVectors: VectorSchemaRoot): List[FieldVector] = dataSchema.map(f => valueVectors.getVector(f.name)).toList

    protected def loadDict(valueVectors: VectorSchemaRoot, dictionary: java.util.Map[java.lang.Long, Dictionary], schema: StructType): List[FieldVector] = {
        val fieldNameToDictionaryEncoding = valueVectors.getSchema.getFields.asScala.map(f => f.getName -> f.getDictionary).toMap

        schema.map(f =>
            Option(fieldNameToDictionaryEncoding(f.name)) match {
                case None => null
                case Some(value) =>
                    if (value.getIndexType.getTypeID != ArrowTypeID.Int)
                        throw new IllegalArgumentException("Wrong index type: " + value.getIndexType)
                    dictionary.get(value.getId).getVector
            }
        ).toList
    }
}
