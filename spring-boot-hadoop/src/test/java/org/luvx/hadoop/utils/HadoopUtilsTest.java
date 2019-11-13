package org.luvx.hadoop.utils;

import org.apache.hadoop.fs.FileSystem;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;

/**
 * @ClassName: org.luvx.hadoop.utils
 * @Description:
 * @Author: Ren, Xie
 * @Date: 2019/11/13 17:04
 */
@RunWith(SpringRunner.class)
@SpringBootTest
public class HadoopUtilsTest {
    FileSystem fs = HadoopConnectionUtils.getFileSystem();

    @Test
    void mkdirTest() throws IOException {
        HadoopUtils.makeDirectory(fs, "helloworld");
    }

    @Test
    void uploadDataTest() throws IOException {
        HadoopUtils.uploadData(fs, "/helloworld", "E:\\hive.log");
    }

    @Test
    void printTest() throws IOException {
        HadoopUtils.print(fs, "/helloworld/word.txt");
    }

    @Test
    void downloadDataTest() throws IOException {
        HadoopUtils.downloadData(fs, "/helloworld/word.txt", "E:\\word.txt");
    }

    @Test
    void deleteFileTest() throws IOException {
        HadoopUtils.deleteFile(fs, "/helloworld");
    }
}