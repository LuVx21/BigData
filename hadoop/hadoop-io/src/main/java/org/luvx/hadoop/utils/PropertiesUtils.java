package org.luvx.hadoop.utils;

import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * @author Ren, Xie
 */
@Slf4j
public class PropertiesUtils {

    public static Properties load(File file) throws IOException {
        try (InputStream in = new FileInputStream(file)) {
            Properties props = new Properties();
            props.load(in);
            return props;
        }
    }

    public static Properties load(String path) {
        try (InputStream in = ClassUtils.getClassLoader().getResourceAsStream(path)) {
            Properties props = new Properties();
            props.load(in);
            return props;
        } catch (IOException e) {
            log.error("加载配置文件异常");
            return null;
        }
    }
}
