package org.arrowspark.spark.config

class DataSource(protected val sourcePath: String) extends Serializable {

    /** Returns 'true' if our source is a file, 'false' otherwise */
    lazy val isFileURI: Boolean = sourcePath.startsWith("file://")

    /** Returns 'true' if our source is a single file, 'false' otherwise */
    lazy val isFileSingle: Boolean = isFileURI && (sourcePath.endsWith(".pq") || sourcePath.endsWith(".csv"))/*FileSupport.file_exists(sourcePath.substring(7))*/

    /**
     * Returns 'true' if our source is a single file, 'false' otherwise
     *
     * '''Note:''' [[isFileSingle]] is not necessarily the inverse of this function.
     * If the user points to e.g. a socket, both functions will return 'false'
     */
    lazy val isFileDir: Boolean = isFileURI && !sourcePath.endsWith(".pq") && !sourcePath.endsWith(".csv")//FileSupport.dir_exists(sourcePath.substring(7))

    lazy val path: String = sourcePath

    override def toString: String = sourcePath
}
