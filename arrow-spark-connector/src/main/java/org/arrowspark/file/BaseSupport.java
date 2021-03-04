package org.arrowspark.file;

import java.util.stream.Stream;

/** Base class for Support classes, which provide methods to write file content for experimenting */
public abstract class BaseSupport {
    /**
     * Write given data to a file
     * @param path Path for file
     * @param values Values for file
     */
    public abstract void write(String path, Stream<Object> values);
}
