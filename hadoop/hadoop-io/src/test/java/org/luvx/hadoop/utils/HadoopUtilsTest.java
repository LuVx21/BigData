package org.luvx.hadoop.utils;

import org.apache.hadoop.fs.FileSystem;
import org.junit.Test;

import java.io.IOException;

/**
 * @author Ren, Xie
 */
public class HadoopUtilsTest {
    FileSystem fs = HadoopConnectionUtils.getFileSystem();

    @Test
    public void mkdirTest() throws IOException {
        HadoopUtils.makeDirectory(fs, "helloworld");
    }

    @Test
    public void uploadDataTest() throws IOException {
        HadoopUtils.uploadData(fs, "/helloworld", "E:\\hive.log");
    }

    @Test
    public void printTest() throws IOException {
        HadoopUtils.print(fs, "/helloworld/word.txt");
    }

    @Test
    public void downloadDataTest() throws IOException {
        HadoopUtils.downloadData(fs, "/helloworld/word.txt", "E:\\word.txt");
    }

    @Test
    public void deleteFileTest() throws IOException {
        HadoopUtils.deleteFile(fs, "/helloworld");
    }
}