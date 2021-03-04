package org.arrowspark.arrow.dataset

import org.apache.arrow.dataset.file.FileFormat

object DatasetFileFormat {
    def apply() = new DatasetFileFormat(FileFormat.PARQUET)
    def apply(fileFormat: FileFormat) = new DatasetFileFormat(fileFormat)

    /** @return Interpreted [[FileFormat]] for given extension-without-leading-dot or string */
    def fromString(s: String): DatasetFileFormat = new DatasetFileFormat(
        s match {
            case "pq" | "parquet" => FileFormat.PARQUET
            case "csv" => FileFormat.CSV
            case _ => throw new IllegalArgumentException("Cannot determine type for '"+s+"'.  Accepted values are: pq, csv")
        }
    )

    /**
     * '''Note:''' Pathlike expressions are supported. This includes URI's
     * @return [[FileFormat]] for given pathlike expression
     */
    def fromPathLike(path: String): DatasetFileFormat = {
        val pattern = ".*\\.(.*)".r
        path match {
            case pattern(extension) => fromString(extension)
            case _ => throw new IllegalArgumentException(s"Cannot determine file type from path '$path'. Make sure this path ends on an extension '.csv' or '.pq'");
        }

    }
}

class DatasetFileFormat(val format: FileFormat) extends Serializable {
    override def toString: String = format match {
        case FileFormat.PARQUET => "parquet"
        case FileFormat.CSV => "csv"
    }

    /** @return Extension string for contained fileformat, minus the leading dot '.' common to file extensions */
    def toExtensionString: String = format match {
        case FileFormat.PARQUET => "pq"
        case FileFormat.CSV => "csv"
    }

    override def equals(obj: Any): Boolean = obj match {
            case a: DatasetFileFormat => a.format == this.format
            case f: FileFormat => f == this.format
            case _ => false
        }
}
