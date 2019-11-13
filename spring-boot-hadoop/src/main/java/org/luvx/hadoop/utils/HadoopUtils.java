package org.luvx.hadoop.utils;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.*;

/**
 * @ClassName: org.luvx.hadoop
 * @Description:
 * @Author: Ren, Xie
 * @Date: 2019/11/13 16:46
 */
@Slf4j
public class HadoopUtils {

    public static boolean makeDirectory(FileSystem fileSystem, String path) throws IOException {
        if (!path.startsWith("/")) {
            return false;
        }
        fileSystem.mkdirs(new Path(fileSystem.getUri() + path));
        return true;
    }

    public static void uploadData(FileSystem fileSystem, String path, String path_local) throws IOException {
        final FSDataOutputStream out = fileSystem.create(new Path(fileSystem.getUri() + path));
        FileInputStream in = new FileInputStream(path_local);
        IOUtils.copyBytes(in, out, 1024, true);
        IOUtils.closeStreams(in, out);
    }

    public static void print(FileSystem fileSystem, String path) throws IOException {
        String file = fileSystem.getUri() + path;
        FSDataInputStream is = fileSystem.open(new Path(file));
        byte[] buff = new byte[1024];
        int length = 0;
        while ((length = is.read(buff)) != -1) {
            System.out.println(new String(buff, 0, length));
        }
        IOUtils.closeStreams(is);
    }

    public static void downloadData(FileSystem fileSystem, String path, String path_local) throws IOException {
        final FSDataInputStream in = fileSystem.open(new Path(fileSystem.getUri() + path));
        OutputStream os = new FileOutputStream(new File(path_local));
        IOUtils.copyBytes(in, os, 1024, true);
        IOUtils.closeStreams(os, in);
    }

    public static void deleteFile(FileSystem fileSystem, String path) throws IOException {
        fileSystem.delete(new Path(fileSystem.getUri() + path), true);
    }
}

