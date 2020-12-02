package org.luvx.hadoop.utils;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * @author Ren, Xie
 */
@Slf4j
public class HadoopUtils {

    public static void mkdir(FileSystem fs, String path) throws IOException {
        if (!path.startsWith("/")) {
            return;
        }
        fs.mkdirs(new Path(path));
    }

    public static void uploadData(FileSystem fs, String localPath, String hdfsPath) throws IOException {
        FSDataOutputStream fos = fs.create(new Path(hdfsPath),
                () -> {
                    // 带进度提醒信息
                    System.out.print(".");
                }
        );
        InputStream in = new BufferedInputStream(new FileInputStream(localPath));
        IOUtils.copyBytes(in, fos, 1024, true);
    }

    public static void print(FileSystem fs, String path) throws IOException {
        try (FSDataInputStream is = fs.open(new Path(path))) {
            IOUtils.copyBytes(is, System.out, 1024, true);
        }
    }

    public static void downloadData(FileSystem fs, String hdfsPath, String localPath) throws IOException {
        log.info("{} -> {}", hdfsPath, localPath);
        Path from = new Path(hdfsPath);
        Path to = new Path(localPath);
        fs.copyToLocalFile(from, to);
    }

    public static void deleteFile(FileSystem fs, String path) throws IOException {
        log.info("delete:{}", path);
        Path p = new Path(path);
        if (fs.exists(p)) {
            fs.delete(new Path(path), true);
        }
    }
}

