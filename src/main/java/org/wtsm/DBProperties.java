package org.wtsm;

import java.util.ResourceBundle;

public class DBProperties {
    /**
     * 根目录
     */
    public static String BASE_PATH = "";
    /**
     * 数据文件名
     */
    public static String DATA_FILE_NAME = "data";
    /**
     * 数据目录名
     */
    public static String DATA_DIR = "data";
    /**
     * 单个数据文件最大长度
     */
    public static long DATA_FILE_MAX_LENGTH = Long.MAX_VALUE;
    /**
     * 磁盘缓存目录
     */
    public static String CACHE_DIR = "cache";
    /**
     * 磁盘缓存文件名
     */
    public static String CACHE_FILE_NAME = "cache";

    public static String getFullDataDir() {
        return BASE_PATH + "/" + DATA_DIR;
    }

    public static String getFullCacheDir() {
        return BASE_PATH + "/" + CACHE_DIR;
    }

    /**
     * 获取db文件名
     */
    public static String getFullDataFilePath(int index) {
        return BASE_PATH + "/" + DATA_DIR + "/" + DATA_FILE_NAME + index;
    }

    public static String getFullMergeFilePath(int index) {
        return BASE_PATH + "/merge/" + index;
    }

    /**
     * 获取磁盘缓存文件名
     */
    public static String getFullCacheFilePath(int index) {
        return BASE_PATH + "/" + CACHE_DIR + "/" + CACHE_FILE_NAME + index;
    }

    public static void initProperties() {
        ResourceBundle rb = ResourceBundle.getBundle("db");
        String dbDir = rb.getString("db_dir");
        if (dbDir.isBlank()) {
            throw new RuntimeException("db_dir不能为空");
        }
        BASE_PATH = dbDir + "\\";

        if (rb.containsKey("data_file_name")) {
            DATA_FILE_NAME = rb.getString("data_file_name");
        }

        if (rb.containsKey("data_dir")) {
            DATA_DIR = rb.getString("data_dir");
        }

        if (rb.containsKey("data_file_max_length")) {
            DATA_FILE_MAX_LENGTH = Long.parseLong(rb.getString("data_file_max_length"));
        }

        if (rb.containsKey("cache_dir")) {
            CACHE_DIR = rb.getString("cache_dir");
        }

        if (rb.containsKey("cache_file_name")) {
            CACHE_FILE_NAME = rb.getString("cache_file_name");
        }
        System.out.println("配置初始化成功");
    }
}
