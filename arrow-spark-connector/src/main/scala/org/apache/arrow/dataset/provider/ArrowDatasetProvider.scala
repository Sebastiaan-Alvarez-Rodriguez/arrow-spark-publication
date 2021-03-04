package org.apache.arrow.dataset.provider

import org.apache.arrow.dataset.jni.NativeDataset
import org.apache.arrow.dataset.source.Dataset
import org.arrowspark.spark.config.ArrowRDDReadConfig

import java.io.Serializable

/**
 * General interface to interact with objects constructing Arrow Datasets
 */
trait ArrowDatasetProvider extends Serializable {
    /** Construct a [[Dataset]], using given `readConfig` settings */
    def provide(readConfig: ArrowRDDReadConfig): NativeDataset
}