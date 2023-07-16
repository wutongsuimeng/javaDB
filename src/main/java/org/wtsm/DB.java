package org.wtsm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;

import java.io.*;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * 文件式数据库
 */
public class DB {
    /**
     * index->key->record
     */
    static Map<Integer, Map<String, Record>> CACHE_MAP = new ConcurrentSkipListMap<>((o1, o2) -> o2 - o1);
    /**
     * 索引缓存
     */
    // 段文件列表
    static LinkedList<Integer> CACHE_INDEXES = new LinkedList<>();

    static {
        DBProperties.initProperties();
        try {
            initCacheAndIndex();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void main(String[] args) throws Exception {
        write("name", "henry");
        write("name", "list");
        write("age", "123");
        System.out.println(read("name"));
    }

    /**
     * 初始化索引
     */
    private static void initCacheAndIndex() throws IOException {
        Path cacheDir = Paths.get(DBProperties.getFullCacheDir());
        if (Files.exists(cacheDir)) {
            try (Stream<Path> list = Files.list(cacheDir)) {
                for (Path path : list.collect(Collectors.toList())) {
                    int index = Integer.parseInt(path.getFileName().toString().replace(DBProperties.CACHE_FILE_NAME, ""));
                    String value = Files.readString(path);
                    CACHE_MAP.put(index, JSON.parseObject(value, new TypeReference<>() {
                    }));
                }
            }
            if (!CACHE_MAP.isEmpty()) {
                CACHE_INDEXES.addAll(CACHE_MAP.keySet().stream().sorted().collect(Collectors.toList()));
            }
        }

        if (CACHE_INDEXES.isEmpty()) {
            CACHE_INDEXES.add(0);
        }
    }

    public static void write(String key, String value) throws Exception {
        String bytes = key + ":" + value + ";";
        // 最新的段文件
        Path curPath = getCurDataPath();
        int curIndex = CACHE_INDEXES.getLast();
        if (Files.notExists(curPath)) {
            createFile(curPath);
        }
        boolean needTask = false;
        if (curPath.toFile().length() > DBProperties.DATA_FILE_MAX_LENGTH) {
            //文件过大，则使用新的段文件
            curIndex++;
            CACHE_INDEXES.addLast(curIndex);
            curPath = getCurDataPath();
            Files.createFile(curPath);
            needTask = true;
        }
        File curFile = curPath.toFile();
        long offset = curFile.length();
        try (FileOutputStream fileOutputStream = new FileOutputStream(curFile, true)) {
            fileOutputStream.write(bytes.getBytes(StandardCharsets.UTF_8));
            fileOutputStream.flush();
        }
        Record record = new Record();
        record.setOffset(offset);
        record.setSize(bytes.length());
        putCacheMap(key, record);

        //保存到磁盘
        int finalCurIndex = curIndex;
        Path cachePath = Paths.get(DBProperties.getFullCacheFilePath(finalCurIndex));
        if (Files.notExists(cachePath)) {
            createFile(cachePath);
        }
        try {
            Files.write(cachePath, JSON.toJSONBytes(CACHE_MAP.get(finalCurIndex)), StandardOpenOption.WRITE);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        //合并与压缩
        if (needTask) {
            task();
        }
    }

    /**
     * 创建文件，并同时创建上级目录
     */
    public synchronized static void createFile(Path path) {
        if (Files.exists(path)) {
            return;
        }
        try {
            Path parent = path.getParent();
            if (Files.notExists(parent)) {
                Files.createDirectories(parent);
            }
            Files.createFile(path);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 保存键值对到内存
     */
    private static void putCacheMap(String key, Record record) {
        for (Map<String, Record> map : CACHE_MAP.values()) {
            if (map.containsKey(key)) {
                map.remove(key);
                break;
            }
        }
        CACHE_MAP.computeIfAbsent(CACHE_INDEXES.getLast(), k -> new HashMap<>()).put(key, record);
    }

    private static void task() {
        try {
            //合并后文件
            int mergeIndex = 0;
            Path mergePath = null;
            FileChannel mergeChannel = null;
            int lastIndex = CACHE_INDEXES.get(CACHE_INDEXES.size() - 2); //记录下合并的位置
            Map<Integer, Map<String, Record>> mergeCacheMap = new TreeMap<>((o1, o2) -> o2 - o1); //合并后的cache
            long mergeOffset = 0; //合并后文件的偏移量
            //最新的不合并
            for (int i = 0; i < CACHE_INDEXES.size() - 1; i++) {
                int index = CACHE_INDEXES.get(i);
                Map<String, Record> recordMap = CACHE_MAP.get(index);
                Path dataPath = Paths.get(DBProperties.getFullDataFilePath(index));
                if (recordMap == null || recordMap.isEmpty() || Files.notExists(dataPath)) {
                    continue;
                }
                try (FileChannel inChannel = FileChannel.open(dataPath, StandardOpenOption.READ)) {
                    for (Map.Entry<String, Record> entry : recordMap.entrySet()) {
                        if (mergePath == null) {
                            // 初始化临时文件
                            mergePath = Paths.get(DBProperties.getFullMergeFilePath(mergeIndex));
                            createFile(mergePath);
                            mergeChannel = FileChannel.open(mergePath, StandardOpenOption.WRITE);
                        }
                        Record record = entry.getValue();
                        long offset = record.getOffset();
                        long size = record.getSize();
                        long transfered = 0;
                        while (transfered < size) {
                            transfered += inChannel.transferTo(transfered + offset, size, mergeChannel);
                        }

                        //保存cache
                        Record newRecord = new Record();
                        newRecord.setOffset(mergeOffset);
                        newRecord.setSize(size);
                        Map<String, Record> newRecordMap = mergeCacheMap.computeIfAbsent(mergeIndex, k -> new HashMap<>());
                        newRecordMap.put(entry.getKey(), newRecord);
                        //更新合并后的偏移量
                        mergeOffset += size;

                        if (mergeChannel.size() > DBProperties.DATA_FILE_MAX_LENGTH) {
                            mergeOffset = 0;
                            mergeChannel.close();
                            mergeIndex++;
                            mergePath = Paths.get(DBProperties.getFullMergeFilePath(mergeIndex));
                            createFile(mergePath);
                            mergeChannel = FileChannel.open(mergePath, StandardOpenOption.WRITE);
                        }
                    }
                } catch (IOException e) {
                    if (mergeChannel != null) {
                        mergeChannel.close();
                    }
                    throw new RuntimeException(e);
                }

            }
            if (mergeChannel != null) {
                mergeChannel.close();
            }

            if (mergePath == null) {
                return;
            }

            //清除并更新
            Iterator<Integer> iterator = CACHE_INDEXES.iterator();
            while (iterator.hasNext()) {
                Integer index = iterator.next();
                Path dataPath = Paths.get(DBProperties.getFullDataFilePath(index));
                if (Files.exists(dataPath)) {
                    Files.delete(dataPath);
                }
                Path cachePath = Paths.get(DBProperties.getFullCacheFilePath(index));
                if (Files.exists(cachePath)) {
                    Files.delete(cachePath);
                }
                CACHE_MAP.remove(index);
                iterator.remove();
                if (index == lastIndex) {
                    break;
                }
            }
            for (int i = mergeIndex; i >= 0; i--) {
                Path dstPath = Paths.get(DBProperties.getFullDataFilePath(i));
                Files.move(Paths.get(DBProperties.getFullMergeFilePath(i)), dstPath);
                if (mergeCacheMap.containsKey(i)) {
                    CACHE_MAP.put(i, mergeCacheMap.get(i));
                }
                CACHE_INDEXES.addFirst(i);
            }

            mergeCacheMap.forEach((k, v) -> {
                Path file = null;
                try {
                    file = Files.createFile(Paths.get(DBProperties.getFullCacheFilePath(k)));
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                try {
                    Files.write(file, JSON.toJSONBytes(v));
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });


        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

    /**
     * 获取最新数据文件
     */
    public static Path getCurDataPath() {
        return Paths.get(DBProperties.getFullDataFilePath(CACHE_INDEXES.getLast()));
    }

    public static String read(String key) throws IOException {
        Record record = null;
        int index = 0;
        for (Map.Entry<Integer, Map<String, Record>> entry : CACHE_MAP.entrySet()) {
            if ((record = entry.getValue().get(key)) != null) {
                index = entry.getKey();
                break;
            }
        }
        if (record == null) {
            return null;
        }
        File curFile = Paths.get(DBProperties.getFullDataFilePath(index)).toFile();
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
}
