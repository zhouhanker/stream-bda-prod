package com.stream.realtime.lululemon.utils;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.wltea.analyzer.lucene.IKAnalyzer;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

public class IKAnalyzerUtils {

    // IKAnalyzer 是线程安全的，可全局单例
    private static final IKAnalyzer ANALYZER_INSTANCE = new IKAnalyzer(true);

    public static List<String> split(String text) {
        if (text == null || text.trim().isEmpty()) {
            return new ArrayList<>();
        }

        List<String> tokens = new ArrayList<>();
        TokenStream tokenStream = null;

        try {
            tokenStream = ANALYZER_INSTANCE.tokenStream("content", new StringReader(text));
            CharTermAttribute termAttr = tokenStream.addAttribute(CharTermAttribute.class);
            tokenStream.reset();

            while (tokenStream.incrementToken()) {
                String token = termAttr.toString();
                if (!token.isEmpty()) {
                    tokens.add(token);
                }
            }

            tokenStream.end();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (tokenStream != null) {
                try {
                    tokenStream.close();
                } catch (IOException ignored) {}
            }
        }
        return tokens;
    }
}
