package org.wtsm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import lombok.Data;
import lombok.ToString;

import java.io.*;
import java.net.URL;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
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
    static ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(Runtime.getRuntime().availableProcessors() * 2, Integer.MAX_VALUE, 60, TimeUnit.SECONDS, new ArrayBlockingQueue<>(1024 * 6));

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
                    int key = Integer.parseInt(path.getFileName().toString().replace(CACHE_FILE_NAME, ""));
                    String value = Files.readString(path);
                    CACHE_MAP.put(key, JSON.parseObject(value, new TypeReference<>() {
                    }));
                }
            }
            if (!CACHE_MAP.isEmpty()) {
                INDEXES.addAll(CACHE_MAP.keySet().stream().sorted().collect(Collectors.toList()));
            }
        }

        if (INDEXES.isEmpty()) {
            INDEXES.add(1);
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
        if (curPath.toFile().length() > FILE_MAX_LENGTH) {
            //文件过大，则使用新的段文件
            curIndex++;
            INDEXES.add(curIndex);
            curPath = getCurDataPath();
            Files.createFile(curPath);

            //合并与压缩
            threadPoolExecutor.execute(() -> {
                task();
            });
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
        putMap(key, record);

        //保存到磁盘
        int finalCurIndex = curIndex;
        threadPoolExecutor.execute(() -> {
            Path cachePath = Paths.get(getBaseCacheFileName() + finalCurIndex);
            if (Files.notExists(cachePath)) {
                createFile(cachePath);
            }
            try {
                Files.write(cachePath, JSON.toJSONBytes(record));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    /**
     * 创建文件，并同时创建上级目录
     */
    public static void createFile(Path path) {
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
    private static void putMap(String key, Record record) {
        Map<String, Record> recordMap = CACHE_MAP.computeIfAbsent(INDEXES.getLast(), k -> new HashMap<>());
        recordMap.put(key, record);
    }

    private static void task() {
        try {
            //合并后文件
            int mergeIndex = 0;
            Path mergePath = Paths.get(BASE_PATH + "/merge/" + mergeIndex);
            FileChannel mergeChannel = FileChannel.open(mergePath, StandardOpenOption.CREATE, StandardOpenOption.WRITE);
            for (int i = 0; i < INDEXES.size() - 1; i++) {
                Map<String, Record> recordMap = CACHE_MAP.get(INDEXES.get(i));
                Path dataPath = Paths.get(getBaseDataFileName() + INDEXES.get(i));
                if (recordMap != null && Files.exists(dataPath)) {
                    try (FileChannel inChannel = FileChannel.open(dataPath, StandardOpenOption.READ)) {
                        for (Map.Entry<String, Record> entry : recordMap.entrySet()) {
                            // TODO: 2023/3/9 需要确保map的数据是顺序的
                            Record record = entry.getValue();
                            long offset = record.getOffset();
                            long size = record.getSize();
                            long transfered = 0;
                            while (transfered < size) {
                                transfered += inChannel.transferTo(transfered + offset, size, mergeChannel);
                            }
                            if (mergeChannel.size() > FILE_MAX_LENGTH) {
                                mergeChannel.close();
                                mergeIndex++;
                                mergePath = Paths.get(BASE_PATH + "/merge/" + mergeIndex);
                                mergeChannel = FileChannel.open(mergePath, StandardOpenOption.CREATE, StandardOpenOption.WRITE);
                            }
                        }
                    } catch (IOException e) {
                        mergeChannel.close();
                        throw new RuntimeException(e);
                    }
                }

            }
            mergeChannel.close();
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
