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
    public static final String CONFIG = "hadoop.properties";

    public static FileSystem getFileSystem() {
        Configuration conf = getConfig();
        return getFileSystem(conf);
    }

    public static FileSystem getFileSystem(Configuration conf) {
        Properties props = PropertiesUtils.load(CONFIG);
        Objects.requireNonNull(props, "加载配置文件异常");

        String root = props.getProperty("hadoop.url");
        String user = props.getProperty("hadoop.user");
        System.setProperty("hadoop.home.dir", props.getProperty("hadoop.home.dir"));

        FileSystem fs = null;
        try {
            fs = FileSystem.get(URI.create(root), conf, user);
        } catch (IOException | InterruptedException e) {
            log.error("获取dfs异常", e);
            return null;
        }
        return fs;
    }

    public static Configuration getConfig() {
        Configuration conf = new Configuration();
        conf.set("dfs.client.use.datanode.hostname", "true");
        return conf;
    }
}


