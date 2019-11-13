package org.luvx.hadoop.utils;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.luvx.hbase.utils.PropertiesUtils;

import java.io.IOException;
import java.net.URI;
import java.util.Properties;

/**
 * @ClassName: org.luvx.hadoop.utils
 * @Description:
 * @Author: Ren, Xie
 * @Date: 2019/11/13 16:51
 */
@Slf4j
public class HadoopConnectionUtils {
    public static FileSystem getFileSystem() {
        Properties props = null;
        try {
            props = PropertiesUtils.load("hadoop.properties");
        } catch (IOException e) {
            log.error("加载配置文件异常");
            return null;
        }
        String root = props.getProperty("hadoop.root");
        String user = props.getProperty("hadoop.user");
        System.setProperty("hadoop.home.dir", props.getProperty("hadoop.home.dir"));

        FileSystem fileSystem = null;
        try {
            fileSystem = FileSystem.get(URI.create(root), new Configuration(), user);
        } catch (IOException | InterruptedException e) {
            log.error("获取dfs异常: {}", e);
            return null;
        }
        return fileSystem;
    }
}


