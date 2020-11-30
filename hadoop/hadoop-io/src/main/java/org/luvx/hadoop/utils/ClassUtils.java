package org.luvx.hadoop.utils;

/**
 * @author Ren, Xie
 */
public class ClassUtils {
    public static Class<?> forName(String name) throws ClassNotFoundException {
        return forName(name, getClassLoader());
    }

    public static Class<?> forName(String name, ClassLoader classLoader) throws ClassNotFoundException {
        return Class.forName(name, true, classLoader);
    }

    public static ClassLoader getClassLoader() {
        return getClassLoader(ClassUtils.class);
    }

    public static ClassLoader getClassLoader(Class<?> cls) {
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        if (cl == null) {
            cl = cls.getClassLoader();
        }
        return cl;
    }
}
