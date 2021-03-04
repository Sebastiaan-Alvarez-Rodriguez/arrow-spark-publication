import org.arrowspark.arrow.dataset.DatasetFileFormat
import org.arrowspark.file.{BaseSupport, FileSupport}

import java.util.concurrent.{ExecutorService, Executors, TimeUnit}

object ArrowSuiteHelper {

    /**
     * Function providing a population buffer for writing a parquet file.
     *
     * '''Note:''' Make sure the values are of type as ordered in the rows of any defined schemas/strongly typed objects.
     * E.g: If the schema specifies we have rows consisting of int, String, long,
     * then the arraybuffer returned must have a repeating sequence of those types
     */
    def generateTempData(amount: Long, start: Long): java.util.stream.Stream[Object] = {
        val end = start+amount
        java.util.stream.LongStream.range(start, start+amount)
            .map(x => start+end-x-1)
            .boxed()
            .flatMap(value => java.util.stream.Stream.of(value.asInstanceOf[Object], s"t$value".asInstanceOf[Object]))
    }



    /**
     * Generates data we test on.
     *
     * @param singleFile If 'true', we generate 1 file.
     *                   Otherwise, we generate 1 file for every partition and build a filestructure `datapath/num_rows/num_partitions/extension/[0-num_partitions-1].extension`
     */
    def generateTestData(baseSupport: BaseSupport, dataFormat: DatasetFileFormat, dataPath: String, partitions: Int, numRows: Long, singleFile: Boolean): Unit = {
        val support = FileSupport.make(baseSupport)
        if (singleFile) {
            println(s"Generating testdata for 1 partition, with $numRows rows")
            support.write(dataPath, generateTempData(numRows, 0))
        } else {
            val splitsize = numRows / partitions
            println(s"Generating testdata for $partitions partitions, each with $splitsize rows (last partition gets ${numRows % partitions} rows)")

            val extension = dataFormat.toExtensionString

            val e: ExecutorService = Executors.newFixedThreadPool(Runtime.getRuntime.availableProcessors())
            for (x <- 0 until partitions) {
                e.execute(() => {
                    val amount =
                        if (x != partitions - 1)
                            splitsize
                        else
                            splitsize + numRows % partitions
                    val start = x * splitsize
                    //                    println(s"Slice $x: $start to ${start + amount} (size=$amount)")
                    support.write(s"$dataPath/$x.$extension", generateTempData(amount, start))

                })
            }
            e.shutdown()
            e.awaitTermination(30, TimeUnit.MINUTES)
        }
    }
}
