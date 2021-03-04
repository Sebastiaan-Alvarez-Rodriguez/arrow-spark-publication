package org.apache.spark.sql.util.compat

import org.apache.spark.sql.catalyst.{JavaTypeInference, ScalaReflection}
import org.apache.spark.sql.types.StructType

import scala.reflect.runtime.universe._


object ReflectionUtil {
    /**
     * Uses built-in Spark reflection method to generate a valid schema
     * @tparam T Type used for reflection
     * @return Reflected schema (might be none)
     */
    def reflectSchema[T <: Product: TypeTag](): Option[StructType] = {
        typeOf[T] match {
            case x if x == typeOf[Nothing] => None
            case _                         => Some(ScalaReflection.schemaFor[T].dataType.asInstanceOf[StructType])
        }
    }


    /**
     * Obtain schema from a BeanClass using Java hacks
     * @param beanClass BeanClass representing schema
     * @tparam T Type of [[Class]]
     * @return Inferred Spark Schema
     */
    def reflectSchema[T](beanClass: Class[T]): StructType = JavaTypeInference.inferDataType(beanClass)._1.asInstanceOf[StructType]

}
