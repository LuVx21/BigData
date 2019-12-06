package org.luvx.utils;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

/**
 * @ClassName: org.luvx.utils
 * @Description:
 * @Author: Ren, Xie
 * @Date: 2019/12/6 10:42
 */
public class TimeUtils {
    public static final DateTimeFormatter dateTimeFormatter1 = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss:SSS");
    public static final DateTimeFormatter dateTimeFormatter2 = DateTimeFormatter.ofPattern("dd/MM/yyyy:HH:mm:ss");

    public static String epochMilli2Time(long time) {
        return dateTimeFormatter1.format(LocalDateTime.ofInstant(Instant.ofEpochMilli(time), ZoneId.systemDefault()));
    }

    public static long str2EpochMilli(String str) {
        LocalDateTime temp = LocalDateTime.parse(str, TimeUtils.dateTimeFormatter2);
        long time = LocalDateTime.from(temp).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
        return time;
    }
}

