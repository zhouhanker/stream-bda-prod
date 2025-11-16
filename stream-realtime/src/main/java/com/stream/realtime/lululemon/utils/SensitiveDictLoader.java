package com.stream.realtime.lululemon.utils;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Set;

/**
 * @Package com.stream.realtime.lululemon.utils.SensitiveDictLoader
 * @Author zhou.han
 * @Date 2025/11/16 22:23
 * @description:
 */
public class SensitiveDictLoader {
    private static final Set<String> FULL_WORD_SET = new HashSet<>();

    static {
        try {
            InputStream is = SensitiveDictLoader.class
                    .getClassLoader()
                    .getResourceAsStream("ik/sensitiveword_ext.dic");

            if (is == null) {
                System.err.println("[SensitiveDictLoader] cannot find resource: sensitiveword_ext.dic");
            }

            if (is != null) {
                try (BufferedReader reader = new BufferedReader(
                        new InputStreamReader(is, StandardCharsets.UTF_8))) {

                    String line;
                    while ((line = reader.readLine()) != null) {
                        line = line.trim();
                        if (!line.isEmpty()) {
                            FULL_WORD_SET.add(line);
                        }
                    }
                }
            }

            System.err.println("[SensitiveDictLoader] loaded words: " + FULL_WORD_SET.size());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static Set<String> getFullWordSet() {
        return FULL_WORD_SET;
    }
}
