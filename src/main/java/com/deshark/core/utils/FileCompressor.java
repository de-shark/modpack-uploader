package com.deshark.core.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;

public class FileCompressor {

    public static File compressFile(File originalFile) throws IOException {

        File compressedFile = File.createTempFile("compressed_", ".tmp");

        try (
                FileInputStream fis = new FileInputStream(originalFile);
                FileOutputStream fos = new FileOutputStream(compressedFile);
                DeflaterOutputStream dos = new DeflaterOutputStream(fos, new Deflater(Deflater.BEST_COMPRESSION))
        ) {
            byte[] buffer = new byte[8192];
            int bytesRead;
            while ((bytesRead = fis.read(buffer)) != -1) {
                dos.write(buffer, 0 , bytesRead);
            }
        }

        return compressedFile;
    }

    public static File decompressFile(File compressedFile) throws IOException {
        File decompressedFile = File.createTempFile("decompressed_", ".json");

        try (FileInputStream fis = new FileInputStream(compressedFile);
             InflaterInputStream iis = new InflaterInputStream(fis);
             FileOutputStream fos = new FileOutputStream(decompressedFile)) {

            byte[] buffer = new byte[8192];
            int bytesRead;
            while ((bytesRead = iis.read(buffer)) != -1) {
                fos.write(buffer, 0, bytesRead);
            }
        }

        return decompressedFile;
    }

    public static boolean shouldCompressFile(File file) {
        String fileName = file.getName().toLowerCase();
        String[] compressibleExtension = {".json", ".txt", ".xml", ".toml", ".js", ".cfg", ".properties"};
        String[] nonCompressibleExtensions = {".png", ".zip", ".jar"};

        for (String ext : nonCompressibleExtensions) {
            if (fileName.endsWith(ext)) {
                return false;
            }
        }

        for (String ext : compressibleExtension) {
            if (fileName.endsWith(ext)) {
                return true;
            }
        }

        return false;
    }
}
