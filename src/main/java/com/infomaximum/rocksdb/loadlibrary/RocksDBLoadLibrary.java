package com.infomaximum.rocksdb.loadlibrary;

import org.rocksdb.RocksDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;

/**
 * Created by kris on 08.08.17.
 */
public class RocksDBLoadLibrary {

    private final static Logger log = LoggerFactory.getLogger(RocksDBLoadLibrary.class);

    private static final String os = System.getProperty("os.name").toLowerCase();
    private static final String arch = System.getProperty("sun.arch.data.model");

    private volatile static boolean isLoad=false;

    public synchronized static void loadLibrary() {
        if (isLoad) return;

        if (isWindows()) {
            if ("64".equals(arch)) {
                loadLibrary("/libs/natives/windows/x64/rpcrt4.dll");
                loadLibrary("/libs/natives/windows/x64/msvcp140.dll");
                loadLibrary("/libs/natives/windows/x64/vcruntime140.dll");
                loadLibrary("/libs/natives/windows/x64/api-ms-win-crt-runtime-l1-1-0.dll");
                loadLibrary("/libs/natives/windows/x64/api-ms-win-crt-stdio-l1-1-0.dll");
                loadLibrary("/libs/natives/windows/x64/api-ms-win-crt-string-l1-1-0.dll");
                loadLibrary("/libs/natives/windows/x64/api-ms-win-crt-convert-l1-1-0.dll");
                loadLibrary("/libs/natives/windows/x64/api-ms-win-crt-time-l1-1-0.dll");
                loadLibrary("/libs/natives/windows/x64/api-ms-win-crt-environment-l1-1-0.dll");
                loadLibrary("/libs/natives/windows/x64/api-ms-win-crt-filesystem-l1-1-0.dll");
                loadLibrary("/libs/natives/windows/x64/api-ms-win-crt-math-l1-1-0.dll");
                loadLibrary("/libs/natives/windows/x64/api-ms-win-crt-heap-l1-1-0.dll");
            } else {
                throw new RuntimeException("Not support arch");
            }
        }

        RocksDB.loadLibrary();

        isLoad=true;
    }

    private static boolean isWindows() {
        return (os.indexOf("win") >= 0);
    }

    private static void loadLibrary(String path) {
        try {
            File file = extractLibraryFromJar(path);
            System.load(file.getAbsolutePath());
        } catch (Throwable e) {
            log.error("Error load library: {}", path, e);
        }
    }

    private static File extractLibraryFromJar(String path) throws IOException {

        if (!path.startsWith("/")) {
            throw new IllegalArgumentException("The path has to be absolute (start with '/').");
        }

        // Obtain filename from path
        String[] parts = path.split("/");
        String filename = (parts.length > 1) ? parts[parts.length - 1] : null;

        // Split filename to prexif and suffix (extension)
        String prefix = "";
        String suffix = null;
        if (filename != null) {
            parts = filename.split("\\.", 2);
            prefix = parts[0];
            suffix = (parts.length > 1) ? "."+parts[parts.length - 1] : null;
        }

        // Check if the filename is okay
        if (filename == null || prefix.length() < 3) {
            throw new IllegalArgumentException("The filename has to be at least 3 characters long.");
        }

        // Prepare temporary file
        File temp = File.createTempFile(prefix, suffix);
        temp.deleteOnExit();

        if (!temp.exists()) {
            throw new FileNotFoundException("File " + temp.getAbsolutePath() + " does not exist.");
        }

        // Prepare buffer for data copying
        byte[] buffer = new byte[1024];
        int readBytes;

        // Open output stream and copy data between source file in JAR and the temporary file
        try (InputStream is = RocksDBLoadLibrary.class.getResourceAsStream(path)) {
            if (is == null) throw new FileNotFoundException("File " + path + " was not found inside JAR.");

            try (OutputStream os = new FileOutputStream(temp)){
                while ((readBytes = is.read(buffer)) != -1) {
                    os.write(buffer, 0, readBytes);
                }
            }
        }

        return temp;
    }
}
