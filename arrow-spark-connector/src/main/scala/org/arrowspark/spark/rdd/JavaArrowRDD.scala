package org.arrowspark.spark.rdd

import org.apache.spark.api.java.JavaRDD
import org.arrowspark.util.RowWrapper

import scala.reflect.ClassTag

/**
 * Java-friendly RDD implementation
 * @param rdd The Scala-RDD to wrap for Java
 * @param classTag Erased Class of RowWrapper-like implementation
 */
case class JavaArrowRDD[D <: RowWrapper](override val rdd: ArrowRDD)(implicit override val classTag: ClassTag[RowWrapper]) extends JavaRDD[RowWrapper](rdd)(classTag) {
}
