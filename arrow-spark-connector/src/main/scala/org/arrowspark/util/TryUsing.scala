package org.arrowspark.util

import scala.util.{Failure, Success, Try}

object TryUsing {

    /** An alternative to Scala 2.13 Using(), which implements a Java try-with-resource-like pattern */
    def autoCloseTry[A <: AutoCloseable, B](closeable: A)(fun: A => B): Try[B] = {
        var t: Throwable = null
        try {
            Success(fun(closeable))
        } catch {
            case funT: Throwable =>
                t = funT
                Failure(t)
        } finally {
            if (t != null) {
                try {
                    closeable.close()
                } catch {
                    case closeT: Throwable =>
                        t.addSuppressed(closeT)
                        Failure(t)
                }
            } else {
                closeable.close()
            }
        }
    }

}
