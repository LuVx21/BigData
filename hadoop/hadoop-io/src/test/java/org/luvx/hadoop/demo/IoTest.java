package org.luvx.hadoop.demo;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.luvx.hadoop.utils.HadoopConnectionUtils;
import org.luvx.hadoop.utils.HadoopUtils;

/**
 * Hadoop HDFS Java API 操作
 *
 * @author Ren, Xie
 */
@Slf4j
public class IoTest {

    private FileSystem fs = null;

    private final String HD_ROOT_DIR    = "/Users/hadoop/";
    private final String LOCAL_ROOT_DIR = "/Users/renxie/";

    @Before
    public void before() {
        log.info("before test");
        fs = HadoopConnectionUtils.getFileSystem();
    }

    @After
    public void after() {
        fs = null;
        log.info("test after");
    }

    /**
     * 创建HDFS目录
     */
    @Test
    public void mkdir() throws Exception {
        HadoopUtils.mkdir(fs, HD_ROOT_DIR + "admin");
    }

    /**
     * 创建文件
     */
    @Test
    public void create() throws Exception {
        try (FSDataOutputStream fos = fs.create(new Path(HD_ROOT_DIR + "1.txt"))) {
            fos.write("hello hadoop".getBytes());
            fos.flush();
        }
    }

    /**
     * 查看HDFS文件的内容
     */
    @Test
    public void cat() throws Exception {
        HadoopUtils.print(fs, HD_ROOT_DIR + "1.txt");
    }

    /**
     * 重命名
     */
    @Test
    public void rename() throws Exception {
        Path oldPath = new Path(HD_ROOT_DIR + "1.txt");
        Path newPath = new Path(HD_ROOT_DIR + "2.txt");
        fs.rename(oldPath, newPath);
    }

    /**
     * 上传文件到HDFS
     */
    @Test
    public void copyFromLocalFile() throws Exception {
        Path localPath = new Path(LOCAL_ROOT_DIR + "1.txt");
        Path hdfsPath = new Path(HD_ROOT_DIR);
        fs.copyFromLocalFile(localPath, hdfsPath);
    }

    /**
     * 上传文件到HDFS
     */
    @Test
    public void copyFromLocalFileWithProgress() throws Exception {
        String localPath = LOCAL_ROOT_DIR + "jdk-11.0.4_linux-x64_bin.tar.gz";
        String hdfsPath = HD_ROOT_DIR + "jdk-11.0.4_linux-x64_bin.tar.gz";
        HadoopUtils.uploadData(fs, localPath, hdfsPath);
    }

    /**
     * 下载HDFS文件
     */
    @Test
    public void copyToLocalFile() throws Exception {
        String hdfsPath = HD_ROOT_DIR + "1.txt";
        String localPath = LOCAL_ROOT_DIR + "2.txt";
        HadoopUtils.downloadData(fs, hdfsPath, localPath);
    }

    /**
     * 查看某个目录下的所有文件
     */
    @Test
    public void listFiles() throws Exception {
        FileStatus[] fileStatuses = fs.listStatus(new Path(HD_ROOT_DIR));
        for (FileStatus fileStatus : fileStatuses) {
            String isDir = fileStatus.isDirectory() ? "文件夹" : "文件";
            short replication = fileStatus.getReplication();
            long len = fileStatus.getLen();
            String path = fileStatus.getPath().toString();
            log.info("{}\t{}\t{}\t{}", isDir, replication, len, path);
        }
    }

    /**
     * 删除
     */
    @Test
    public void delete() throws Exception {
        HadoopUtils.deleteFile(fs, HD_ROOT_DIR);
    }
}
