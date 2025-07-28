package com.deshark;

import com.deshark.core.ConfigManager;
import com.deshark.core.schemas.*;
import com.deshark.core.storage.CloudStorageProvider;
import com.deshark.core.storage.StorageProviderFactory;
import com.deshark.core.task.ModpackFileUploadTask;
import com.deshark.core.utils.FileUtil;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;

public class Main {

    private static final Logger logger = LoggerFactory.getLogger(Main.class);
    private static final ObjectMapper mapper = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
    private static CloudStorageProvider storageProvider;

    public static void main(String[] args) {

        ConfigManager configManager = new ConfigManager();

        // fetch config
        String secretId = configManager.getSecretId();
        String secretKey = configManager.getSecretKey();
        String region = configManager.getRegion();
        String bucketName = configManager.getBucketName();
        String downloadUrl = configManager.getDownloadUrl();
        String sourceDirStr = configManager.getSourceDir();
        String sourceServerStr = configManager.getSourceServerDir();
        String sourceClientStr = configManager.getSourceClientDir();
        String projectId = configManager.getProjectId();
        String versionName = configManager.getVersionName();

        Path sourceDir;
        Path sourceServerDir;
        Path sourceClientDir;

        try {
            sourceDir = createDirectoryIfNotExists(sourceDirStr);
            sourceServerDir = createDirectoryIfNotExists(sourceServerStr);
            sourceClientDir = createDirectoryIfNotExists(sourceClientStr);
        } catch (IOException e) {
            logger.error("Source directory error", e);
            return;
        }

        // set libraries(whats the use)
        Map<String, String> libraries = new HashMap<>();
        libraries.put("net.minecraft", "1.7.10");
        libraries.put("net.minecraftforge", "10.13.4.1614");

        storageProvider = StorageProviderFactory.createProvider(
                StorageProviderFactory.StorageType.TENCENT_COS,
                secretId, secretKey, region, bucketName
        );

        String modpackKey = "stable/" + projectId + "/versions/" + versionName + "/modpack.json";
        String versionsKey = "stable/" + projectId + "/versions.json";
        String metaKey = "stable/" + projectId + "/meta.json";

        // check version
        try {
            checkExistingVersions(metaKey, versionsKey, versionName);
        } catch (IOException e) {
            logger.error("Version check failed for metaKey: {}, versionsKey: {}, versionName: {}",
                    metaKey, versionsKey, versionName, e);
            return;
        }

        List<Path> fileList;
        List<Path> serverFileList;
        List<Path> clientFileList;
        try {
            fileList = FileUtil.collectFiles(sourceDir);
            serverFileList = FileUtil.collectFiles(sourceServerDir);
            clientFileList = FileUtil.collectFiles(sourceClientDir);
        } catch (IOException e) {
            logger.error("Failed to collect files", e);
            return;
        }
        logger.info("Found {} common files", fileList.size());
        logger.info("Found {} server files", serverFileList.size());
        logger.info("Found {} client files", clientFileList.size());
        logger.info("Total files: {}", fileList.size() + serverFileList.size() + clientFileList.size());

        long startTime = System.currentTimeMillis();
        List<ModpackFile> results = new ArrayList<>();

        ExecutorService executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() * 2);
        try {
            List<CompletableFuture<ModpackFile>> commonFutures = fileList.stream()
                    .map(file -> new ModpackFileUploadTask(storageProvider, file, getRelativePath(file, sourceDir), downloadUrl, "common", 3)
                            .executeAsync(executor))
                    .toList();
            List<CompletableFuture<ModpackFile>> serverFutures = serverFileList.stream()
                    .map(file -> new ModpackFileUploadTask(storageProvider, file, getRelativePath(file, sourceServerDir), downloadUrl, "server", 3)
                            .executeAsync(executor))
                    .toList();

            List<CompletableFuture<ModpackFile>> clientFutures = clientFileList.stream()
                    .map(file -> new ModpackFileUploadTask(storageProvider, file, getRelativePath(file, sourceClientDir), downloadUrl, "client", 3)
                            .executeAsync(executor))
                    .toList();

            List<CompletableFuture<ModpackFile>> allFutures = new ArrayList<>();
            allFutures.addAll(commonFutures);
            allFutures.addAll(serverFutures);
            allFutures.addAll(clientFutures);

            for (CompletableFuture<ModpackFile> future : allFutures) {
                try {
                    results.add(future.get());
                } catch (ExecutionException | InterruptedException e) {
                    logger.error("File upload failed", e);
                    return;
                }
            }
        } finally {
            executor.shutdown();
            try {
                if (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
                    executor.shutdownNow();
                }
            } catch (InterruptedException e) {
                executor.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }

        long endTime = System.currentTimeMillis();
        logger.info("Files upload completed in {} ms", endTime - startTime);

        // meta files
        Modpack modpack = new Modpack(results, versionName, libraries);

        uploadConfigFile(modpack, modpackKey);

        String currentDate = LocalDateTime.now()
                .format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
        String modpackUrl = downloadUrl + "/" + modpackKey;
        String changelogUrl = downloadUrl + "/" + "stable/" + projectId + "/versions/" + versionName + "/changelog.json";
        VersionInfo lastestVersion = new VersionInfo(
                versionName, currentDate, modpackUrl, changelogUrl
        );

        List<VersionInfo> newVersions = getExistingVersions(versionsKey);
        newVersions.add(lastestVersion);
        Versions versions = new Versions(newVersions);

        uploadConfigFile(versions, versionsKey);

        String VersionsUrl = downloadUrl + "/" + versionsKey;

        MetaFile meta = new MetaFile(VersionsUrl, lastestVersion);

        uploadConfigFile(meta, metaKey);

        String metaUrl = downloadUrl + "/" + metaKey;
        logger.info("================");
        logger.info("发布完成! 耗时: {}s", (System.currentTimeMillis() - startTime) / 1000.0);
        logger.info("上传文件: {}个", ModpackFileUploadTask.getUploadedFilesCount());
        logger.info("跳过文件: {}个", ModpackFileUploadTask.getSkippedFilesCount());
        logger.info("版本: {}", versionName);
        logger.info("meta.json: {}", metaUrl);

        storageProvider.shutdown();
    }

    private static Path createDirectoryIfNotExists(String pathStr) throws IOException {
        if (pathStr == null || pathStr.isEmpty()) {
            throw new IllegalArgumentException("Path cannot be null or empty");
        }
        Path path = Paths.get(pathStr);
        if (!Files.exists(path)) {
            Files.createDirectories(path);
        }
        return path;
    }

    private static void checkExistingVersions(String metaKey, String versionsKey, String versionName) throws IOException {
        try (InputStream is = storageProvider.getObjectStream(metaKey)) {
            if (is != null) {
                MetaFile meta = mapper.readValue(is, MetaFile.class);
                String latestVersionName = meta.latestVersion().versionName();
                logger.info("Latest Version Name: {}", latestVersionName);
            } else {
                logger.info("No meta data found");
            }
        }
        try (InputStream is = storageProvider.getObjectStream(versionsKey)) {
            if (is != null) {
                Versions versions = mapper.readValue(is, Versions.class);
                boolean duplicate = versions.versions().stream()
                        .anyMatch(v -> v.versionName().equals(versionName));
                if (duplicate) {
                    logger.error("Duplicate version name: {}", versionName);
                    throw new IOException("Duplicate version name");
                }
            }
        }
    }

    private static List<VersionInfo> getExistingVersions(String versionsKey) {
        if (storageProvider.fileExists(versionsKey)) {
            try (InputStream versionsStream = storageProvider.getObjectStream(versionsKey)) {
                return mapper.readValue(versionsStream, Versions.class).versions();
            } catch (IOException e) {
                throw new RuntimeException("Failed to read existing versions", e);
            }
        }
        return new ArrayList<>();
    }

    private static <T> void uploadConfigFile(T config, String key) {
        try {
            String configJson = mapper.writeValueAsString(config);
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DeflaterOutputStream dos = new DeflaterOutputStream(baos, new Deflater(Deflater.BEST_COMPRESSION));
            dos.write(configJson.getBytes(StandardCharsets.UTF_8));
            dos.finish();
            dos.close();

            byte[] configBytes = baos.toByteArray();

            InputStream is = new ByteArrayInputStream(configBytes);
            storageProvider.upload(is, key, true);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static String getRelativePath(Path file, Path baseDir) {
        return baseDir.relativize(file).toString().replace("\\", "/");
    }
}