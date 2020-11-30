package org.luvx.hadoop.utils;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;
import java.net.URI;
import java.util.Objects;
import java.util.Properties;

/**
 * @author Ren, Xie
 */
@Slf4j
public class HadoopConnectionUtils {
    public static FileSystem getFileSystem() {
        Properties props = PropertiesUtils.load("hadoop.properties");
        Objects.requireNonNull(props, "加载配置文件异常");

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


