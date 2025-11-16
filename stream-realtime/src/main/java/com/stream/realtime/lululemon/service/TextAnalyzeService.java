package com.stream.realtime.lululemon.service;

import com.stream.realtime.lululemon.utils.IKAnalyzerUtils;
import com.stream.realtime.lululemon.utils.SensitiveWordUtils;

import java.util.List;

/**
 * @Package com.stream.realtime.lululemon.service.TextAnalyzeService
 * @Author zhou.han
 * @Date 2025/11/16 22:20
 * @description:
 */
public class TextAnalyzeService {
    public static class AnalyzeResult {
        public final List<String> tokens;
        public final List<String> sensitiveWords;

        public AnalyzeResult(List<String> tokens, List<String> sensitiveWords) {
            this.tokens = tokens;
            this.sensitiveWords = sensitiveWords;
        }

        @Override
        public String toString() {
            return "tokens=" + tokens + ", sensitiveWords=" + sensitiveWords;
        }
    }

    /**
     * 最终统一入口：既分词，又检测敏感词
     */
    public static AnalyzeResult analyze(String text) {
        List<String> tokens = IKAnalyzerUtils.split(text);
        List<String> sensitiveWords = SensitiveWordUtils.detect(text);

        return new AnalyzeResult(tokens, sensitiveWords);
    }
}
