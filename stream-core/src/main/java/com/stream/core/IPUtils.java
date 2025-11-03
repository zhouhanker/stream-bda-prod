package com.stream.core;

import org.lionsoul.ip2region.xdb.IPv4;
import org.lionsoul.ip2region.xdb.LongByteArray;
import org.lionsoul.ip2region.xdb.Searcher;
import org.lionsoul.ip2region.xdb.Version;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * @Package com.stream.core.IPUtils
 * @Author zhou.han
 * @Date 2025/11/1 23:40
 * @description:
 */
public class IPUtils {

    private static final Logger logger = LoggerFactory.getLogger(IPUtils.class);
    private static volatile Searcher searcher;
    private static final Object lock = new Object();
    private static final String DB_PATH = ConfigUtils.getString("ip2region.path");
    private static Searcher getSearcher() throws IOException {
        if (searcher == null) {
            synchronized (lock) {
                if (searcher == null) {
                    IPv4 ipv4 = Version.IPv4;
                    LongByteArray cBuff = Searcher.loadContentFromFile(DB_PATH);
                    searcher = Searcher.newWithBuffer(ipv4, cBuff);
                }
            }
        }
        return searcher;
    }

    public static String ip2Region(String ip) {
        try {
            long start = System.nanoTime();
            String region = getSearcher().search(ip);
            long cost = (System.nanoTime() - start) / 1000;
//             logger.info("IP: {}, Region: {}, Cost: {} µs", ip, region, cost);
            return region;
        } catch (Exception e) {
            return "未知";
        }
    }

    public static void close() {
        if (searcher != null) {
            try {
                searcher.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }



}
