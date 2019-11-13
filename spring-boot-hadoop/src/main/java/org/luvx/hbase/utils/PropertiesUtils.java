package org.luvx.hbase.utils;


import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * @ClassName: org.luvx.hbase
 * @Description:
 * @Author: Ren, Xie
 * @Date: 2019/11/12 15:53
 */
public class PropertiesUtils {

    public static Properties load(File file) throws IOException {
        try (InputStream in = new FileInputStream(file)) {
            Properties props = new Properties();
            props.load(in);
            return props;
        }
    }

    public static Properties load(String path) throws IOException {
        try (InputStream in = ClassUtils.getClassLoader().getResourceAsStream(path)) {
            Properties props = new Properties();
            props.load(in);
            return props;
        }
    }
}
