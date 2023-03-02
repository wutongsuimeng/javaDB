package org.wtsm;

import lombok.Data;
import lombok.ToString;

import java.io.*;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

/**
 * 文件式数据库
 */
public class DB {
    /**
     * index->key->record
     */
    static Map<Integer,Map<String, Record>> MAP = new HashMap<>();
    static Path path;
    static String BASE_PATH = "";
    // 段文件列表
    static LinkedList<Integer> INDEXES = new LinkedList<>();
    static String FILE_NAME = "database";
    static long FILE_MAX_LENGTH = Long.MAX_VALUE;

    public static void main(String[] args) throws Exception {
        String packageName = DB.class.getPackage().getName();
        URL resource = Thread.currentThread().getContextClassLoader().getResource("");
        BASE_PATH = resource.getPath().substring(1) + packageName.replace(".", "/");

//        write("name","henry");
//        write("name","list");
//        write("age","123");
//        System.out.println(read("name"));
    }

    public static void write(String key, String value) throws Exception {
        String bytes = key + ":" + value + ";";
        File curFile = getCurFile();
        long offset = curFile.length();
        try (FileOutputStream fileOutputStream = new FileOutputStream(curFile, true)) {
            fileOutputStream.write(bytes.getBytes(StandardCharsets.UTF_8));
            fileOutputStream.flush();
        }
        Record record = new Record();
        record.setOffset(offset);
        record.setSize(bytes.length());
        MAP.put(key, record);
    }

    private static File getCurFile() throws IOException {
        // TODO: 2023/3/2 一开始INDEXES为空的情况
        // 最新的段文件
        Path curPath = Paths.get(BASE_PATH + "/" + FILE_NAME + INDEXES.getLast());
        if (Files.notExists(curPath)) {
            Files.createFile(curPath);
        }
        File curFile = curPath.toFile();
        if (curFile.length() > FILE_MAX_LENGTH) {
            //文件过大，则使用新的段文件
            INDEXES.add(INDEXES.getLast() + 1);
            curPath = Paths.get(BASE_PATH + "/" + FILE_NAME + INDEXES.getLast());
            curFile = curPath.toFile();
            Files.createFile(curPath);
        }
        return curFile;
    }

    public static String read(String key) throws IOException {
        Record record = MAP.get(key);
        if (record == null) {
            return null;
        }
        System.out.println(record);
        try (FileInputStream fileInputStream = new FileInputStream(path.toFile())) {
            long offset = record.getOffset();
            ;
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
