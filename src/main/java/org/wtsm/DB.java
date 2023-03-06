package org.wtsm;

import lombok.Data;
import lombok.ToString;

import java.io.*;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

/**
 * 文件式数据库
 */
public class DB {
    // TODO: 2023/3/5 并发问题
    /**
     * index->key->record
     */
    static Map<Integer, Map<String, Record>> MAP = new TreeMap<>((o1, o2) -> o2-o1);
    static String BASE_PATH = "";
    // 段文件列表
    static LinkedList<Integer> INDEXES = new LinkedList<>();
    static String FILE_NAME = "database";
    static long FILE_MAX_LENGTH = 6L;
    //    static long FILE_MAX_LENGTH = Long.MAX_VALUE;
    static String BASE_FILE;

    static {
        String packageName = DB.class.getPackage().getName();
        URL resource = Thread.currentThread().getContextClassLoader().getResource("");
        BASE_PATH = resource.getPath().substring(1) + packageName.replace(".", "/");
        BASE_FILE = BASE_PATH + "/" + FILE_NAME;
    }

    public static void main(String[] args) throws Exception {
        write("name", "henry");
        write("name", "list");
        write("age", "123");
        System.out.println(read("name"));
    }

    public static void write(String key, String value) throws Exception {
        String bytes = key + ":" + value + ";";
        File curFile = getOrCreateCurFile();
        long offset = curFile.length();
        try (FileOutputStream fileOutputStream = new FileOutputStream(curFile, true)) {
            fileOutputStream.write(bytes.getBytes(StandardCharsets.UTF_8));
            fileOutputStream.flush();
        }
        Record record = new Record();
        record.setOffset(offset);
        record.setSize(bytes.length());
        putMap(key, record);
    }

    /**
     * 保存键值对到内存
     *
     * @param key
     * @param record
     */
    private static void putMap(String key, Record record) {
        Map<String, Record> recordMap = MAP.computeIfAbsent(INDEXES.getLast(), k -> new HashMap<>());
        recordMap.put(key, record);
    }

    private static File getOrCreateCurFile() throws IOException {
        if (INDEXES.isEmpty()) {
            // TODO: 2023/3/5 以后需要考虑从磁盘读取
            INDEXES.add(1);
        }
        // 最新的段文件
        Path curPath = Paths.get(BASE_FILE + INDEXES.getLast());
        if (Files.notExists(curPath)) {
            Files.createFile(curPath);
        }
        File curFile = curPath.toFile();
        if (curFile.length() > FILE_MAX_LENGTH) {
            //文件过大，则使用新的段文件
            INDEXES.add(INDEXES.getLast() + 1);
            curPath = Paths.get(BASE_FILE + INDEXES.getLast());
            curFile = curPath.toFile();
            Files.createFile(curPath);
        }
        return curFile;
    }

    private static File getFile(int index) {
        // 最新的段文件
        Path curPath = Paths.get(BASE_FILE + index);
        return curPath.toFile();
    }

    public static String read(String key) throws IOException {
        Record record = null;
        int index=0;
        for (Map.Entry<Integer, Map<String, Record>> entry : MAP.entrySet()) {
            if ((record = entry.getValue().get(key)) != null) {
                index=entry.getKey();
                break;
            }
        }
        if (record == null) {
            return null;
        }
        File curFile = getFile(index);
        try (FileInputStream fileInputStream = new FileInputStream(curFile)) {
            long offset = record.getOffset();
            long size = record.getSize();
            byte[] b = new byte[(int) (size)];
            long readSize = 0;
            fileInputStream.skip(offset);
            while (readSize < size) {
                readSize += fileInputStream.read(b, (int) readSize, (int) (size - readSize));
            }
            return new String(b);
        }
    }

    @Data
    @ToString
    static class Record {
        private long offset; //偏移量
        private long size; //大小
    }
}
