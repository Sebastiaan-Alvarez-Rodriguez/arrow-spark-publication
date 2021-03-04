package org.arrowspark.file;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.function.Consumer;
import java.util.stream.Stream;

/**
 * Utility class for writing Parquet files using Avro based tools.
 */
public class ParquetSupport extends BaseSupport {
    protected Schema schema;
    protected CompressionCodecName compressionCodecName;

    public ParquetSupport(Schema schema, CompressionCodecName compressionCodecName) {
        this.schema = schema;
        this.compressionCodecName = compressionCodecName;
    }
    public ParquetSupport(Schema schema) {
        this.schema = schema;
        this.compressionCodecName = CompressionCodecName.UNCOMPRESSED;
    }

    public ParquetSupport(String schema, CompressionCodecName compressionCodecName) {
        this(parseSchema(schema), compressionCodecName);
    }
    public ParquetSupport(String schema) {
        this(parseSchema(schema));
    }

    /** Determines compression codec to use based on provided token. Reviews end of item */
    public static CompressionCodecName compressionCodecFromString(String token) {
        if (token.endsWith("uncompressed"))
            return CompressionCodecName.UNCOMPRESSED;
        if (token.endsWith("snappy"))
            return CompressionCodecName.SNAPPY;
        if (token.endsWith("gzip"))
            return CompressionCodecName.GZIP;
        if (token.endsWith("lz0"))
            return CompressionCodecName.LZO;
        if (token.endsWith("brotli"))
            return CompressionCodecName.BROTLI;
        if (token.endsWith("lz4"))
            return CompressionCodecName.LZ4;
        if (token.endsWith("zstd"))
            return CompressionCodecName.ZSTD;
        return CompressionCodecName.UNCOMPRESSED;
    }

    public static String compressionCodecToString(CompressionCodecName name) {
        return name.name().toLowerCase();
    }

    /** Parse string schema */
    protected static Schema parseSchema(String schema) {
        return new org.apache.avro.Schema.Parser().parse(schema);
    }

    /** Read a schema from an avro-based parquet file */
    protected static Schema readSchema(String path) throws Exception {
        return new org.apache.avro.Schema.Parser().parse(Paths.get(path).toFile());
    }


    private static void forEachRecordDo(Schema schema, Consumer<GenericRecord> consumer, Stream<Object> values) {
        final int fieldCount = schema.getFields().size();

        Iterator<Object> iterator = values.iterator();

        while(iterator.hasNext()) {
            final GenericRecord record = new GenericData.Record(schema);
            for (int y = 0; y < fieldCount; ++y)
                record.put(y, iterator.next()); // Using access mechanism we should explicitly not use, to avoid schema initialization errors
            consumer.accept(record);
        }
        values.close();
    }

    /** Write given objects as parquet file to given output path */
    @Override
    public void write(String path, Stream<Object> values) {
        try {
            ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(new org.apache.hadoop.fs.Path(path)).withSchema(schema).withCompressionCodec(compressionCodecName).build();
            forEachRecordDo(schema, genericRecord -> {
                try {
                    writer.write(genericRecord);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }, values);
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
