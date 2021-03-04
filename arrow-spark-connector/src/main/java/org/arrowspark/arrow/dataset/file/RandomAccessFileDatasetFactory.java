package org.arrowspark.arrow.dataset.file;

import org.apache.arrow.dataset.file.FileFormat;
import org.apache.arrow.dataset.file.JniWrapper;
import org.apache.arrow.dataset.jni.NativeDatasetFactory;
import org.apache.arrow.dataset.jni.NativeMemoryPool;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.MemoryUtil;
import org.arrowspark.arrow.dataset.DatasetFileFormat;

/** JNI frontend to construct [[RandomAccessFileDataset]] */
public class RandomAccessFileDatasetFactory extends NativeDatasetFactory {


    /**
     * Constructs [[RandomAccessFileDatasetFactory]] which will read entire file.
     * @param allocator Allocator passed to use for [[RandomAccessFileDataset]]
     * @param fileFormat Type of file we read
     * @param path Location of file(s)
     */
    public RandomAccessFileDatasetFactory(BufferAllocator allocator, NativeMemoryPool memoryPool, FileFormat fileFormat, String path) {
        super(allocator, memoryPool, createNative(fileFormat, path, -1L, -1L));
    }
    public RandomAccessFileDatasetFactory(BufferAllocator allocator, NativeMemoryPool memoryPool, DatasetFileFormat fileFormat, String path) {
        super(allocator, memoryPool, createNative(fileFormat.format(), path, -1L, -1L));
    }

    @SuppressWarnings("unused")
    public RandomAccessFileDatasetFactory(BufferAllocator allocator, FileFormat fileFormat, String path) {
        this(allocator, MemoryUtil.getMemoryPool(), fileFormat, path);
    }
    public RandomAccessFileDatasetFactory(BufferAllocator allocator, DatasetFileFormat fileFormat, String path) {
        this(allocator, MemoryUtil.getMemoryPool(), fileFormat, path);
    }

    /**
     * Constructs new [[RandomAccessFileDatasetFactory]] which will read only a part of a file
     * @param allocator Allocator passed to use for [[RandomAccessFileDataset]]
     * @param fileFormat Type of file we read
     * @param path Location of file
     * @param startOffset Byte-offset in file to start reading
     * @param length Number of bytes to read
     */
    @SuppressWarnings("unused")
    public RandomAccessFileDatasetFactory(BufferAllocator allocator, NativeMemoryPool memoryPool, FileFormat fileFormat, String path, long startOffset, long length) {
        super(allocator, memoryPool, createNative(fileFormat, path, startOffset, length));
    }
    public RandomAccessFileDatasetFactory(BufferAllocator allocator, NativeMemoryPool memoryPool, DatasetFileFormat fileFormat, String path, long startOffset, long length) {
        super(allocator, memoryPool, createNative(fileFormat.format(), path, startOffset, length));
    }

    protected static long createNative(FileFormat format, String path, long startOffset, long length) {
        return JniWrapper.get().makeSingleFileDatasetFactory("file://" + path,
                format.id(), startOffset, length);
    }
}
