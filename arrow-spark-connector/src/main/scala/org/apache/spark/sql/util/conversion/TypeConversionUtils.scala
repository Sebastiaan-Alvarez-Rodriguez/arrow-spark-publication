/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.util.conversion

import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.{DateUnit, FloatingPointPrecision, TimeUnit}
import org.apache.spark.sql.catalyst.CatalystTypeConverters
import org.apache.spark.sql.types._

/** TODO: Support more types, e.g. composed types. See [[CatalystTypeConverters]] for composed types options */
object TypeConversionUtils {
    /**
     * @param dt Type to check
     * @return `true` if given Spark Catalyst type can be converted to an equivalent Arrow type, `false` otherwise
     */
    def isSupported(dt: DataType): Boolean = dt match {
        case BooleanType => true
        case ByteType => true
        case ShortType => true
        case IntegerType => true
        case LongType => true
        case FloatType => true
        case DoubleType => true
        case StringType => true
        case BinaryType => true
        case DecimalType.Fixed(_, _) => true
        case DateType => true
        case TimestampType => true
        case _ => false
    }

    /**
     * @param dt Type to check
     * @return `true` if given Arrow type can be converted to an equivalent Spark Catalyst type, `false` otherwise
     */
    //noinspection ScalaUnusedSymbol
    def isSupported(dt: ArrowType): Boolean = dt match {
        case ArrowType.Bool.INSTANCE => true
        case int: ArrowType.Int if int.getIsSigned && int.getBitWidth == 8 => true
        case int: ArrowType.Int if int.getIsSigned && int.getBitWidth == 8 * 2 => true
        case int: ArrowType.Int if int.getIsSigned && int.getBitWidth == 8 * 4 => true
        case int: ArrowType.Int if int.getIsSigned && int.getBitWidth == 8 * 8 => true
        case float: ArrowType.FloatingPoint if float.getPrecision == FloatingPointPrecision.SINGLE => true
        case float: ArrowType.FloatingPoint if float.getPrecision == FloatingPointPrecision.DOUBLE => true
        case ArrowType.Utf8.INSTANCE => true
        case ArrowType.Binary.INSTANCE => true
        case d: ArrowType.Decimal => true
        case date: ArrowType.Date if date.getUnit == DateUnit.DAY => true
        case ts: ArrowType.Timestamp if ts.getUnit == TimeUnit.MICROSECOND => true
        case _ => false
    }

    /** Maps data type from Spark to Arrow */
    def toArrowType(dt: DataType, timeZoneId: String): ArrowType = dt match {
        case BooleanType => ArrowType.Bool.INSTANCE
        case ByteType => new ArrowType.Int(8, true)
        case ShortType => new ArrowType.Int(8 * 2, true)
        case IntegerType => new ArrowType.Int(8 * 4, true)
        case LongType => new ArrowType.Int(8 * 8, true)
        case FloatType => new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)
        case DoubleType => new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)
        case StringType => ArrowType.Utf8.INSTANCE
        case BinaryType => ArrowType.Binary.INSTANCE
        case DecimalType.Fixed(precision, scale) => new ArrowType.Decimal(precision, scale)
        case DateType => new ArrowType.Date(DateUnit.DAY)
        case TimestampType =>
            if (timeZoneId == null)
                throw new UnsupportedOperationException(s"${TimestampType.catalogString} must supply timeZoneId parameter")
            else
                new ArrowType.Timestamp(TimeUnit.MICROSECOND, timeZoneId)
        case _ =>
            throw new UnsupportedOperationException(s"Unsupported data type: ${dt.catalogString}")
    }

    /** Maps data types from Arrow to Spark */
    def fromArrowType(dt: ArrowType): DataType = dt match {
        case ArrowType.Bool.INSTANCE => BooleanType
        case int: ArrowType.Int if int.getIsSigned && int.getBitWidth == 8 => ByteType
        case int: ArrowType.Int if int.getIsSigned && int.getBitWidth == 8 * 2 => ShortType
        case int: ArrowType.Int if int.getIsSigned && int.getBitWidth == 8 * 4 => IntegerType
        case int: ArrowType.Int if int.getIsSigned && int.getBitWidth == 8 * 8 => LongType
        case float: ArrowType.FloatingPoint if float.getPrecision == FloatingPointPrecision.SINGLE => FloatType
        case float: ArrowType.FloatingPoint if float.getPrecision == FloatingPointPrecision.DOUBLE => DoubleType
        case ArrowType.Utf8.INSTANCE => StringType
        case ArrowType.Binary.INSTANCE => BinaryType
        case d: ArrowType.Decimal => DecimalType(d.getPrecision, d.getScale)
        case date: ArrowType.Date if date.getUnit == DateUnit.DAY => DateType
        case ts: ArrowType.Timestamp if ts.getUnit == TimeUnit.MICROSECOND => TimestampType
        case _ => throw new UnsupportedOperationException(s"Unsupported data type: $dt")
    }


}
