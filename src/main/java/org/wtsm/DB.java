package org.wtsm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;

import java.io.*;
import java.net.URL;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * 文件式数据库
 */
public class DB {
    // TODO: 2023/3/5 并发问题
    /**
     * index->key->record
     */
    static Map<Integer, Map<String, Record>> CACHE_MAP = new TreeMap<>((o1, o2) -> o2 - o1);
    /**
     * 根目录
     */
    static String BASE_PATH = "";
    /**
     * 索引缓存
     */
    // 段文件列表
    static LinkedList<Integer> INDEXES = new LinkedList<>();
    /**
     * 数据文件名
     */
    static String DATA_FILE_NAME = "db";
    /**
     * 数据目录名
     */
    static String DATA_DIR = "data";
    /**
     * 单个数据文明最大长度
     */
    static long FILE_MAX_LENGTH = 6L;
    //    static long FILE_MAX_LENGTH = Long.MAX_VALUE;
    /**
     * 磁盘缓存目录
     */
    static String CACHE_DIR = "cache";
    /**
     * 磁盘缓存文件名
     */
    static String CACHE_FILE_NAME = "cache";
//    static ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(1, 1, 60, TimeUnit.SECONDS, new ArrayBlockingQueue<>(1024 * 6));

    public static void main(String[] args) throws Exception {
        init();
        write("name", "henry");
        write("name", "list");
        write("age", "123");
        System.out.println(read("name"));
    }

    /**
     * 初始化索引
     */
    private static void init() {
        initPro();
        try {
            initCacheAndIndex();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static void initPro() {
        String packageName = DB.class.getPackage().getName();
        URL resource = Thread.currentThread().getContextClassLoader().getResource("");
        BASE_PATH = resource.getPath().substring(1) + packageName.replace(".", "/");
//        BASE_FILE = BASE_PATH + "/" + DATA_FILE_NAME;
    }

    /**
     * 获取db文件名
     */
    public static String getBaseDataFileName() {
        return BASE_PATH + "/" + DATA_DIR + "/" + DATA_FILE_NAME;
    }

    /**
     * 获取磁盘缓存文件名
     */
    public static String getBaseCacheFileName() {
        return BASE_PATH + "/" + CACHE_DIR + "/" + CACHE_FILE_NAME;
    }

    private static void initCacheAndIndex() throws IOException {
        Path cacheDir = Paths.get(BASE_PATH + "/" + CACHE_DIR);
        if (Files.exists(cacheDir)) {
            try (Stream<Path> list = Files.list(cacheDir)) {
                for (Path path : list.collect(Collectors.toList())) {
                    int index = Integer.parseInt(path.getFileName().toString().replace(CACHE_FILE_NAME, ""));
                    String value = Files.readString(path);
                    CACHE_MAP.put(index, JSON.parseObject(value, new TypeReference<>() {
                    }));
                }
            }
            if (!CACHE_MAP.isEmpty()) {
                INDEXES.addAll(CACHE_MAP.keySet().stream().sorted().collect(Collectors.toList()));
            }
        }

        if (INDEXES.isEmpty()) {
            INDEXES.add(0);
        }
    }

    public static void write(String key, String value) throws Exception {
        String bytes = key + ":" + value + ";";
        // 最新的段文件
        Path curPath = getCurDataPath();
        int curIndex = INDEXES.getLast();
        if (Files.notExists(curPath)) {
            createFile(curPath);
        }
        boolean needTask = false;
        if (curPath.toFile().length() > FILE_MAX_LENGTH) {
            //文件过大，则使用新的段文件
            curIndex++;
            INDEXES.addLast(curIndex);
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
//        threadPoolExecutor.execute(() -> {
        Path cachePath = Paths.get(getBaseCacheFileName() + finalCurIndex);
        if (Files.notExists(cachePath)) {
            createFile(cachePath);
        }
        try {
            Files.write(cachePath, JSON.toJSONBytes(CACHE_MAP.get(finalCurIndex)), StandardOpenOption.WRITE);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
//        });

        //合并与压缩
//            threadPoolExecutor.execute(() -> {
//                task();
//            });
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
        CACHE_MAP.computeIfAbsent(INDEXES.getLast(), k -> new HashMap<>()).put(key, record);
    }

    // TODO: 2023/3/12 单线程实现
    private static void task() {
        try {
            //合并后文件
            int mergeIndex = 0;
            Path mergePath = null;
            FileChannel mergeChannel = null;
            int lastIndex = INDEXES.get(INDEXES.size() - 2); //记录下合并的位置
            Map<Integer, Map<String, Record>> mergeCacheMap = new TreeMap<>((o1, o2) -> o2 - o1); //合并后的cache
            long mergeOffset = 0; //合并后文件的偏移量
            //最新的不合并
            for (int i = 0; i < INDEXES.size() - 1; i++) {
                int index = INDEXES.get(i);
                Map<String, Record> recordMap = CACHE_MAP.get(index);
                Path dataPath = Paths.get(getBaseDataFileName() + index);
                if (recordMap == null || recordMap.isEmpty() || Files.notExists(dataPath)) {
                    continue;
                }
                // TODO: 2023/3/12 如何避免重复合并操作
                try (FileChannel inChannel = FileChannel.open(dataPath, StandardOpenOption.READ)) {
                    for (Map.Entry<String, Record> entry : recordMap.entrySet()) {
                        if (mergePath == null) {
                            // 初始化临时文件
                            mergePath = Paths.get(BASE_PATH + "/merge/" + mergeIndex);
                            createFile(mergePath);
                            mergeChannel = FileChannel.open(mergePath, StandardOpenOption.WRITE);
                        }
                        // TODO: 2023/3/9 需要确保map的数据是顺序的
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

                        if (mergeChannel.size() > FILE_MAX_LENGTH) {
                            mergeOffset = 0;
                            mergeChannel.close();
                            mergeIndex++;
                            mergePath = Paths.get(BASE_PATH + "/merge/" + mergeIndex);
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
            Iterator<Integer> iterator = INDEXES.iterator();
            while (iterator.hasNext()) {
                Integer index = iterator.next();
                Files.delete(Paths.get(getBaseDataFileName() + index));
                Files.delete(Paths.get(getBaseCacheFileName() + index));
                CACHE_MAP.remove(index);
                iterator.remove();
                if (index == lastIndex) {
                    break;
                }
            }
            for (int i = mergeIndex; i >= 0; i--) {
                Path dstPath = Paths.get(getBaseDataFileName() + i);
                Files.move(Paths.get(BASE_PATH + "/merge/" + i), dstPath);
                if (mergeCacheMap.containsKey(i)) {
                    CACHE_MAP.put(i, mergeCacheMap.get(i));
                }
                INDEXES.addFirst(i);
            }

            mergeCacheMap.forEach((k, v) -> {
                Path file = null;
                try {
                    file = Files.createFile(Paths.get(getBaseCacheFileName() + k));
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
        return Paths.get(getBaseDataFileName() + INDEXES.getLast());
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
        File curFile = Paths.get(getBaseDataFileName() + index).toFile();
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
