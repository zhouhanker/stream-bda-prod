package com.stream.core;

import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Date;

/**
 * 日期工具类
 * time: 2021/9/9 13:37 className: DateTimeUtils.java
 *
 * @author han.zhou
 * @version 1.0.0
 */
public final class DateTimeUtils {
    public final static String YYYY_MM_DD_HH_MM_SS = "yyyy-MM-dd HH:mm:ss";
    private static final DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private static final DateTimeFormatter dtf_ds = DateTimeFormatter.ofPattern("yyyyMMdd");

    public static String format(Date date) {
        return format(date, YYYY_MM_DD_HH_MM_SS);
    }

    public static String format(Date date, String format) {
        SimpleDateFormat formatter = new SimpleDateFormat(format);
        return formatter.format(date);
    }
    public static String tsToDate(Long ts) {

        if (ts == null){
            return null;
        }

        long timestamp = ts;
        String strTs = String.valueOf(ts);
        if (strTs.length() == 10){
            timestamp = ts * 1000;
        } else if (strTs.length() != 13) {
            return null;
        }
        Date dt = new Date(timestamp);
        LocalDateTime localDateTime = LocalDateTime.ofInstant(dt.toInstant(), ZoneId.systemDefault());
        return dtf_ds.format(localDateTime);
    }

    public static String ds2DorisPt(String ds){
        return LocalDate.parse(ds,dtf_ds).format(dtf);
    }

    public static void main(String[] args) {
        System.err.println(ds2DorisPt("20251103"));
    }
}
