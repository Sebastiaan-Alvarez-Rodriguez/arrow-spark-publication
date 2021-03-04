package org.arrowspark.util


class FastSkipIterator[T](itr: Iterator[T], startIndex: Long = 0, jumpSize: Long, sink: T => Unit) extends Iterator[T] {
    for (x <- 0L until startIndex) {
        if (itr.hasNext)
            sink(itr.next)
        else
            throw new IndexOutOfBoundsException(s"Cannot skip to startIndex $startIndex, only have $x items")
    }

    override def hasNext: Boolean = itr.hasNext

    override def next(): T = {
        val x = itr.next
        for (x <- 0L until jumpSize)
            if (itr.hasNext)
                itr.next
            else
                throw new IndexOutOfBoundsException(s"Cannot skip to startIndex $startIndex, only have $x items")

        x
    }
}
