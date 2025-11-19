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


    public static long normalizeTs(String tsStr) {

        if (tsStr == null || tsStr.trim().isEmpty()) {
            return System.currentTimeMillis();
        }

        tsStr = tsStr.trim();

        try {
            // 如果是纯数字
            if (tsStr.matches("^\\d+$")) {

                int len = tsStr.length();

                // 10位 → 秒
                if (len == 10) {
                    return Long.parseLong(tsStr) * 1000L;
                }
                // 13位 → 毫秒
                if (len == 13) {
                    return Long.parseLong(tsStr);
                }
                // 超过13位（微秒/纳秒） → 截取前13位
                if (len > 13) {
                    return Long.parseLong(tsStr.substring(0, 13));
                }

                // 少于10位 → 当秒处理
                return Long.parseLong(tsStr) * 1000L;
            }

            // 如果包含小数点（double 格式）
            if (tsStr.matches("^\\d+\\.\\d+$")) {
                double d = Double.parseDouble(tsStr);

                // 判断大致是秒还是毫秒
                if (d < 1e11) { // 小于 1e11 基本不可能是毫秒时间戳
                    return (long) (d * 1000L); // 秒 → 毫秒
                } else {
                    return (long) d; // 毫秒 → 直接转 long（小数部分自然丢失）
                }
            }

            // 其他格式（例如字符串日期）尝试 parse
            return Long.parseLong(tsStr);

        } catch (Exception e) {
            return System.currentTimeMillis();
        }
    }



    public static void main(String[] args) {
        System.err.println(tsToDate(1761948706L));
        System.err.println(ds2DorisPt("20251101"));
    }

}
