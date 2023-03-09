package org.wtsm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import lombok.Data;
import lombok.ToString;

import java.io.*;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

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
    static char deleteMark=' '; //删除标志
    static ThreadPoolExecutor threadPoolExecutor=new ThreadPoolExecutor(Runtime.getRuntime().availableProcessors()*2, Integer.MAX_VALUE,60, TimeUnit.SECONDS,new ArrayBlockingQueue<>(1024*6));

    static {
        String packageName = DB.class.getPackage().getName();
        URL resource = Thread.currentThread().getContextClassLoader().getResource("");
        BASE_PATH = resource.getPath().substring(1) + packageName.replace(".", "/");
        BASE_FILE = BASE_PATH + "/" + FILE_NAME;
    }

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
        try {
            Path mapPath=Paths.get(BASE_PATH+"/"+"map");
            if(Files.exists(mapPath)){
                Map<Integer, String> map = Files.list(mapPath).collect(Collectors.toMap(f -> Integer.parseInt(f.getFileName().toString().replace("map", "")), f -> {
                    try {
                        return Files.readString(f);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }));
                //直接放进map中
                map.forEach((k,v)->{
                    //todo 暂时使用json
                    MAP.put(k, JSON.parseObject(v, new TypeReference<>() {}));
                });

                INDEXES.addAll(map.keySet().stream().sorted().collect(Collectors.toList()));
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
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

        //保存到磁盘
        Path mapPath=Paths.get(BASE_PATH+"/"+"map");
        if(Files.notExists(mapPath)){
            Files.createDirectory(mapPath);
        }
        Path mapFilePath=Paths.get(BASE_PATH+"/map/map"+INDEXES.getLast());
        if(Files.notExists(mapFilePath)){
            Files.createFile(mapFilePath);
        }
        Files.write(mapFilePath, JSON.toJSONBytes(record));
    }

    /**
     * 保存键值对到内存
     *
     * @param key
     * @param record
     */
    private static void putMap(String key, Record record) {
        // TODO: 2023/3/7 需要考虑键值对已在map中的情况，需要删除
        Map<String, Record> recordMap = MAP.computeIfAbsent(INDEXES.getLast(), k -> new HashMap<>());
        Record last=recordMap.get(key);
        if(last!=null){
            //添加删除标志
            
        }
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
            
            //合并与压缩
            threadPoolExecutor.execute(() -> {
                //合并后文件
                Path mergePath=Paths.get(BASE_FILE+"merge");
                FileOutputStream mergeOut=new FileOutputStream(mergePath.toFile());
                for (int i = 0; i < INDEXES.size()-1; i++) {
                    Map<String, Record> recordMap = MAP.get(INDEXES.get(i));
                    Path path=Paths.get(BASE_FILE+INDEXES.get(i));
                    if (recordMap != null && Files.exists(path)) {
                        try(FileInputStream in=new FileInputStream(path.toFile())){
                            for (Map.Entry<String, Record> entry : recordMap.entrySet()) {
                                // TODO: 2023/3/9 需要确保map的数据是顺序的
                                Record record = entry.getValue();
                                long offset = record.getOffset();
                                long size = record.getSize();
                                byte[] b = new byte[(int) (size)];
                                long readSize = 0;
                                in.skip(offset);
                                while (readSize < size) {
                                    readSize += in.read(b, (int) readSize, (int) (size - readSize));
                                }

                                // TODO: 2023/3/9 需要确定这个方法
                                in.reset();
                                mergeOut.write(b);
                                // todo 有一个trans的方法能直接传输

                            }
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }
                }
            });
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
