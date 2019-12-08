package org.luvx.utils;

import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAdjusters;

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

    /**
     * 本月第一天
     *
     * @return
     */
    public static LocalDate firstDayOfThisMonth() {
        LocalDate today = LocalDate.now();
        return today.with(TemporalAdjusters.firstDayOfMonth());
    }

    /**
     * 本月第N天
     *
     * @param n
     * @return
     */
    public static LocalDate dayOfThisMonth(int n) {
        LocalDate today = LocalDate.now();
        return today.withDayOfMonth(n);
    }

    /**
     * 本月最后一天
     *
     * @return
     */
    public static LocalDate lastDayOfThisMonth() {
        LocalDate today = LocalDate.now();
        return today.with(TemporalAdjusters.lastDayOfMonth());
    }

    /**
     * 本月第一天的开始时间
     *
     * @return
     */
    public static LocalDateTime startOfThisMonth() {
        return LocalDateTime.of(firstDayOfThisMonth(), LocalTime.MIN);
    }


    /**
     * 本月最后一天的结束时间
     *
     * @return
     */
    public static LocalDateTime endOfThisMonth() {
        return LocalDateTime.of(lastDayOfThisMonth(), LocalTime.MAX);
    }
}

