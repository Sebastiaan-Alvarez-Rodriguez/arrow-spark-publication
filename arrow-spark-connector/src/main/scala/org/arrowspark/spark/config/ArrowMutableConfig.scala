package org.arrowspark.spark.config

import java.util

trait ArrowMutableConfig {

    /**
     * Defines Self as a type that can be used to return a copy of the object i.e. a different instance of the same type
     */
    type Self <: ArrowMutableConfig

    /**
     * Creates a new config with the options applied
     *
     * @param key the configuration key
     * @param value the configuration value
     * @return an updated config
     */
    def withOption(key: String, value: String): Self

    /**
     * Creates a new config with the options applied
     *
     * @param options a map of options to be applied to the config
     * @return an updated config
     */
    def withOptions(options: collection.Map[String, String]): Self

    /**
     * Creates a map of options representing this config.
     *
     * @return the map representing the config
     */
    def asOptions: collection.Map[String, String]

    /**
     * Creates a new config with the options applied
     *
     * @param options a map of options to be applied to the config
     * @return an updated config
     */
    def withOptions(options: util.Map[String, String]): Self

    /**
     * Creates a map of options representing the configuration
     *
     * @return the map representing the configuration values
     */
    def asJavaOptions: util.Map[String, String]
}
