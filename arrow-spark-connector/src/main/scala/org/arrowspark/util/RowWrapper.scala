package org.arrowspark.util

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.SpecializedGetters
import org.apache.spark.sql.catalyst.util.{ArrayData, MapData}
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.types.{DataType, Decimal, StructType}
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}

/**
 * Wrapper class around generic [[InternalRow]], which exposes it as a regular [[Row]].
 * This is a useful utility when we really must use regular rows instead of internal ones.
 * However, there is a small cost associated with wrapping.
 *
 * '''Note:''' Make sure that the `schema` and `converters` parameters are reused between calls!
 * Especially doing a per-row construction of converters is very costly in performance.
 *
 * @param internalRow The `InternalRow` to wrap
 * @param schema Schema of the row
 * @param converters The [[CatalystTypeConverters.createToScalaConverter(DataType)]]
 *
 * @example To get the converters, use a simple lookup like:
 *
 * ' val convertors : Array[Any => Any] = schema.map(field => CatalystTypeConverters.createToScalaConverter(field.dataType)).toArray} '
 */
class RowWrapper(internalRow: InternalRow, schema: StructType, converters: Array[Any => Any]) extends Row with SpecializedGetters with Serializable {
    //Consider: UnsafeRow supports Java’s Externalizable and Kryo’s KryoSerializable serialization/deserialization protocols.
    // Note: The slowness comes from Spark converting this thing to InternalRow format and then compressing the space needed by using raw memory.
    //      You can see this by uncommenting the assert(false) in [[get(i: Int)]] and checking the stacktrace

    // Need a new plan probably, because this is not easily fixed
    // Info on how Spark reads parquet: https://animeshtrivedi.github.io/spark-parquet-reading
    // This way of hooking into Spark, basically the way of the mongoDB people, is too slow.
    override def length: Int = internalRow.numFields

    /** We use the converter at ordinal `i` to convert the dataType of non-primitive types */
    override def get(i: Int): Any = converters(i)(internalRow.get(i, schema(i).dataType))



    override def getAs[T](i: Int): T = converters(i)(internalRow.get(i, schema(i).dataType)).asInstanceOf[T]

    override def getAs[T](fieldName: String): T = super.getAs(fieldName)

    override def isNullAt(i: Int): Boolean = internalRow.isNullAt(i)

    /** We directly access `Boolean` primitive type in the underlying container */
    override def getBoolean(i: Int): Boolean = internalRow.getBoolean(i)

    /** We directly access `Byte` primitive type in the underlying container */
    override def getByte(i: Int): Byte = internalRow.getByte(i)

    /** We directly access `Short` primitive type in the underlying container */
    override def getShort(i: Int): Short = internalRow.getShort(i)

    /** We directly access `Int` primitive type in the underlying container */
    override def getInt(i: Int): Int = internalRow.getInt(i)

    /** We directly access `Long` primitive type in the underlying container */
    override def getLong(i: Int): Long = internalRow.getLong(i)

    /** We directly access `Float` primitive type in the underlying container */
    override def getFloat(i: Int): Float = internalRow.getFloat(i)

    /** We directly access `Double` primitive type in the underlying container */
    override def getDouble(i: Int): Double = internalRow.getDouble(i)

    /** Method to copy this row. Note that we do not copy the converters, as they are from the immutable `Array` type */
    override def copy(): Row = new RowWrapper(internalRow.copy(), schema.copy(), converters)

    override def getDecimal(ordinal: Int, precision: Int, scale: Int): Decimal = internalRow.getDecimal(ordinal, precision, scale)

    override def getUTF8String(ordinal: Int): UTF8String = internalRow.getUTF8String(ordinal)

    override def getBinary(ordinal: Int): Array[Byte] = internalRow.getBinary(ordinal)

    override def getInterval(ordinal: Int): CalendarInterval = internalRow.getInterval(ordinal)

    override def getStruct(ordinal: Int, numFields: Int): InternalRow = internalRow.getStruct(ordinal, numFields)

    override def getArray(ordinal: Int): ArrayData = internalRow.getArray(ordinal)

    override def getMap(ordinal: Int): MapData = internalRow.getMap(ordinal)

    override def get(ordinal: Int, dataType: DataType): AnyRef = internalRow.get(ordinal, dataType)
}
