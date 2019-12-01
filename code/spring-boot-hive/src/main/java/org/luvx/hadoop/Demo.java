package org.luvx.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.*;
import java.net.URI;

/**
 * @ClassName: org.luvx.hadoop
 * @Description:
 * @Author: Ren, Xie
 * @Date: 2019/11/4 11:43
 */
public class Demo {
    private static final String url = "hdfs://192.168.0.229:8020";

    public static void print() throws IOException {
        String file = "hdfs://192.168.0.229:8020/word.txt";
        FileSystem fs = FileSystem.get(URI.create(file), new Configuration());
        FSDataInputStream is = fs.open(new Path(file));
        byte[] buff = new byte[1024];
        int length = 0;
        // 打印每一行文字内容
        while ((length = is.read(buff)) != -1) {
            System.out.println(new String(buff, 0, length));
        }
    }

    public static void makeDirectory() throws IOException {
        FileSystem fileSystem = FileSystem.get(URI.create("hdfs://192.168.0.229:8020/"), new Configuration());
        fileSystem.mkdirs(new Path("hdfs://192.168.0.229:8020/helloworld"));
    }

    public static void uploadData() throws IOException {
        FileSystem fileSystem = FileSystem.get(URI.create("hdfs://192.168.0.229:8020/"), new Configuration());
        final FSDataOutputStream out = fileSystem.create(new Path("hdfs://192.168.0.229:8020/helloworld/word.txt"));
        FileInputStream in = new FileInputStream("/Users/dev/Desktop/word.txt");
        IOUtils.copyBytes(in, out, 1024, true);
    }

    public static void downloadData() throws IOException {
        FileSystem fileSystem = FileSystem.get(URI.create("hdfs://192.168.0.229:8020/"), new Configuration());
        final FSDataInputStream in = fileSystem.open(new Path("hdfs://192.168.0.229:8020/helloworld/word.txt"));
        OutputStream os = new FileOutputStream(new File("/Users/dev/Desktop/word2.txt"));
        IOUtils.copyBytes(in, os, 1024, true);
    }

    public static void deleteFile() throws IOException {
        FileSystem fileSystem = FileSystem.get(URI.create("hdfs://192.168.0.229:8020/"), new Configuration());
        fileSystem.delete(new Path("hdfs://192.168.0.229:8020/helloworld"), true);
    }
}
