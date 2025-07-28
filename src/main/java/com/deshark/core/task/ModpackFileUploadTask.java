package com.deshark.core.task;

import com.deshark.core.schemas.ModpackFile;
import com.deshark.core.storage.CloudStorageProvider;
import com.deshark.core.utils.FileUtil;
import com.deshark.core.utils.HashUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

public class ModpackFileUploadTask {
    private static final Logger log = LoggerFactory.getLogger(ModpackFileUploadTask.class);
    private final CloudStorageProvider storage;
    private final Path file;
    private final String relativePath;
    private final String downloadUrl;
    private final String dist;
    private final int maxRetries;
    private final AtomicInteger currentRetry = new AtomicInteger(0);
    private static final AtomicInteger uploadedFiles = new AtomicInteger(0);
    private static final AtomicInteger skippedFiles = new AtomicInteger(0);

    public ModpackFileUploadTask(CloudStorageProvider storage, Path file, String relativePath, String downloadUrl, String dist, int maxRetries) {
        this.storage = storage;
        this.file = file;
        this.relativePath = relativePath;
        this.downloadUrl = downloadUrl;
        this.dist = dist;
        this.maxRetries = maxRetries;
    }

    public CompletableFuture<ModpackFile> executeAsync(Executor executor) {
        return CompletableFuture
                .supplyAsync(this::executeSync, executor)
                .handleAsync((result, throwable) -> {
                    if (throwable != null) {
                        if (currentRetry.incrementAndGet() <= maxRetries) {
                            log.warn("Retry {} for file {}", currentRetry, relativePath);
                            try {
                                Thread.sleep(1000L * currentRetry.get());
                            } catch (InterruptedException e) {
                                throw new RuntimeException(e);
                            }
                            return executeAsync(executor).join();
                        } else {
                            log.error("Failed after {} retries for file {}", maxRetries, relativePath, throwable);
                            throw new RuntimeException("Failed after " + maxRetries + " retries", throwable);
                        }
                    }
                    return result;
                });
    }

    public ModpackFile executeSync() {
        Path fileToUpload = null;
        boolean shouldCompress = false;
        try {
            String hash = HashUtil.SHA256(file);
            String key = generateStorageKey(hash);
            String link = downloadUrl + "/" + key;
            shouldCompress = FileUtil.shouldCompressFile(file);
            if (storage.fileExists(key)) {
                log.info("Skip upload because it already exists: {}", relativePath);
                skippedFiles.incrementAndGet();
            } else {
                fileToUpload = file;
                if (shouldCompress) {
                    fileToUpload = FileUtil.compressFile(file);
                }
                storage.upload(fileToUpload, key, shouldCompress);
                log.info("Upload completed: {}", relativePath);
                uploadedFiles.incrementAndGet();
            }
            return new ModpackFile(relativePath, hash, link, Files.size(file), dist, shouldCompress);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            try {
                if (shouldCompress && fileToUpload != null && !fileToUpload.equals(file)) {
                    Files.deleteIfExists(fileToUpload);
                }
            } catch (IOException e) {
                log.warn("Failed to delete temp file {}", relativePath, e);
            }
        }
    }

    private static String generateStorageKey(String fileHash) {
        if (fileHash == null || fileHash.length() < 8) {
            throw new IllegalArgumentException("Invalid file hash");
        }
        return fileHash.charAt(0) + "/" +
                fileHash.substring(1, 3) + "/" +
                fileHash.substring(3, 7) + "/" +
                fileHash.substring(8);
    }

    public static int getUploadedFilesCount() {
        return uploadedFiles.get();
    }

    public static int getSkippedFilesCount() {
        return skippedFiles.get();
    }
}
