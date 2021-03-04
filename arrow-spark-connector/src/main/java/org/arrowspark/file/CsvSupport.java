package org.arrowspark.file;

import javax.annotation.Nullable;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

/** Object to handle writing CSV files, optionally with a header (like seen in many CSV files) */
public class CsvSupport extends BaseSupport {
    protected @Nullable String header;
    protected List<Class<?>> classes;

    /**
     * @param classes List of classes to use for casting data. If a table has columns: int, string, then pass a list containing int.class and string.class in this specific order
     * @param header The header of the CSV file. This will be written first, if non-null
     */
    public CsvSupport(List<Class<?>> classes, @Nullable String header) {
        this.header = header;
        this.classes = classes;
    }

    /**
     * Works just like other implementation, without header
     * @see #CsvSupport(List, String)
     */
    public CsvSupport(List<Class<?>> classes) {
        this.classes = classes;
        this.header = null;
    }

    @Override
    public void write(String path, Stream<Object> values) {
        File directory = Paths.get(path).getParent().toFile();
//        noinspection ResultOfMethodCallIgnored
        directory.mkdirs();
        try (FileWriter writer = new FileWriter(path)) {
            if (header != null)
                writer.write(header+"\n");
            Iterator<Object> iterator = values.iterator();
            int col = 0;
            while (iterator.hasNext()) {
                Object o = iterator.next();
                writer.write(classes.get(col).cast(o).toString());
                col+= 1;
                if (col == classes.size()) {
                    writer.write("\n");
                    col = 0;
                } else {
                    writer.write(",");
                }
            }
            values.close();
            if (col != 0)
                throw new RuntimeException("Iterator values should be divisible by amount of columns in schema ("+classes.size()+")");

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
