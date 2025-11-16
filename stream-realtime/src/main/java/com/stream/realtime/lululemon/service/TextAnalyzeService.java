package com.stream.realtime.lululemon.service;

import com.stream.realtime.lululemon.utils.IKAnalyzerUtils;
import com.stream.realtime.lululemon.utils.SensitiveWordUtils;
import com.stream.realtime.lululemon.utils.SensitiveDictLoader;
import java.util.Set;
import java.util.List;
import java.util.stream.Collectors;

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

    // ======================= 原有方法保持不变 ==========================
    public static AnalyzeResult analyze(String text) {
        List<String> tokens = IKAnalyzerUtils.split(text);
        List<String> sensitiveWords = SensitiveWordUtils.detect(text);
        return new AnalyzeResult(tokens, sensitiveWords);
    }

    // ======================= 新增：返回 P0 的方法 ========================
    public static AnalyzeResult analyzeP0(String text) {
        List<String> tokens = IKAnalyzerUtils.split(text);
        List<String> all = SensitiveWordUtils.detect(text);

        // P0：来自 sensitiveword_ext.dic
        Set<String> p0Set = SensitiveDictLoader.getFullWordSet();
        List<String> p0 = all.stream()
                .filter(p0Set::contains)
                .collect(Collectors.toList());

        return new AnalyzeResult(tokens, p0);
    }

    // ======================= 新增：返回 P1 的方法 ========================
    public static AnalyzeResult analyzeP1(String text) {
        List<String> tokens = IKAnalyzerUtils.split(text);
        List<String> all = SensitiveWordUtils.detect(text);

        // P0 词典
        Set<String> p0Set = SensitiveDictLoader.getFullWordSet();

        // P1：不在 sensitiveword_ext.dic 中的命中
        List<String> p1 = all.stream()
                .filter(w -> !p0Set.contains(w))
                .collect(Collectors.toList());

        return new AnalyzeResult(tokens, p1);
    }
}
