package org.apache.arrow.dataset.provider
import org.apache.arrow.dataset.jni.NativeDataset
import org.apache.arrow.memory.MemoryUtil
import org.arrowspark.arrow.dataset.DatasetFileFormat
import org.arrowspark.arrow.dataset.file.RandomAccessFileDatasetFactory
import org.arrowspark.spark.config.{ArrowRDDReadConfig, DataSource}
import org.arrowspark.util.TryUsing

class ArrowDefaultDatasetProvider extends ArrowDatasetProvider {

    override def provide(config: ArrowRDDReadConfig): NativeDataset = {
        var path = config.dataSource.path
        val dataSource: DataSource = new DataSource(path)

        if (!dataSource.isFileSingle) { // We are dealing with a multi-file setup
            if (config.datasetProviderOptions.contains(ArrowRDDReadConfig.datasetProviderOptionsPathExtraProperty)) {
                path += config.datasetProviderOptions(ArrowRDDReadConfig.datasetProviderOptionsPathExtraProperty) //use user-provided extra path part to construct a file path
            } else {
                throw new RuntimeException(s"""
                                              |Arrow currently only accepts file paths, while current path does not point to a file: ${dataSource.path}.
                                              |If your partitioner does not query the ArrowDataset before compute time except to get schemas, it is safe to define the relative path to any datafile.
                                              |Use '.withOption(s"$${ArrowRDDReadConfig.datasetProviderOptionsProperty}.$${ArrowRDDReadConfig.datasetProviderOptionsPathExtraProperty}", "path/to/file")'""".stripMargin)
            }
        }
        val fType: DatasetFileFormat = fileType(config, path)
        if (config.dataSource.isFileURI)
            path = path.substring(7)
        TryUsing.autoCloseTry(new RandomAccessFileDatasetFactory(MemoryUtil.getAllocator, fType, path))(factory => factory.finish()).get
    }

    protected def fileType(config: ArrowRDDReadConfig, path: String): DatasetFileFormat = {
        if (config.datasetProviderOptions.contains(ArrowRDDReadConfig.datasetProviderOptionsFileTypeProperty)) {
            DatasetFileFormat.fromString(config.datasetProviderOptions(ArrowRDDReadConfig.datasetProviderOptionsFileTypeProperty))
        } else {
            if (config.dataSource.isFileURI)
                DatasetFileFormat.fromPathLike(path)
            else
                throw new RuntimeException("Cannot determine file type when passing something that is not a file URI. " +
                    "Consider supplying the type in the ArrowRDDReadConfig, using `readconfig.withFileFormat(format)`")
        }
    }
}


case object ArrowDefaultDatasetProvider extends ArrowDefaultDatasetProvider