package org.arrowspark.file;

import java.io.File;
import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.stream.Stream;

public class FileSupport {
    protected BaseSupport writer;

    public FileSupport(BaseSupport writer) {
        this.writer = writer;
    }

    public static FileSupport make(BaseSupport writer) {
        return new FileSupport(writer);
    }

    public void write(String path, Stream<Object> values) {
        writer.write(path, values);
    }


    public static boolean file_exists(String path, String... more) {
        return Files.isRegularFile(Paths.get(path, more));
    }

    public static boolean dir_exists(String path, String... more) {
        return Files.isDirectory(Paths.get(path, more));
    }

    public static boolean file_delete(String path) {
        return new File(path).delete();
    }
    public static boolean dir_delete(String path) throws IOException {
        if (dir_exists(path))
            deleteDirectoryWalkTree(path);
        return true;
    }

    public static String get_parent_dir(String path, String... more) {
        return Paths.get(path, more).getParent().toString();
    }

    public static String get_filename(String path, String... more) {
        return Paths.get(path, more).getFileName().toString();
    }

    public static boolean is_filepath(String path, String... more) {
        return Paths.get(path, more).endsWith("/");
    }

    public static boolean is_dirpath(String path, String... more) {
        return !is_filepath(path, more);
    }

    private static void deleteDirectoryWalkTree(String p, String... more) throws IOException {
        Path path = Paths.get(p, more);
        FileVisitor visitor = new SimpleFileVisitor<Path>() {

            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                Files.delete(file);
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {
                Files.delete(file);
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                if (exc != null) {
                    throw exc;
                }
                Files.delete(dir);
                return FileVisitResult.CONTINUE;
            }
        };
        Files.walkFileTree(path, visitor);
    }
}
