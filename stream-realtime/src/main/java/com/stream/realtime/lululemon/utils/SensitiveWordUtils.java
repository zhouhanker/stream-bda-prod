package com.stream.realtime.lululemon.utils;

import com.github.houbb.sensitive.word.bs.SensitiveWordBs;

import java.util.*;
import java.util.stream.Collectors;

public class SensitiveWordUtils {

    private static final SensitiveWordBs BS = SensitiveWordBs.newInstance().init();

    /**
     * 返回最终敏感词列表：优先完整词，并做“最长匹配”去重
     */
    public static List<String> detect(String text) {
        if (text == null || text.isEmpty()) {
            return Collections.emptyList();
        }

        // 1. DFA 返回的原始命中（可能是短词）
        List<String> dfaList = BS.findAll(text);

        // 2. 自定义词典里的完整词（从 sensitiveword_ext.dic 读取）
        Set<String> fullWords = SensitiveDictLoader.getFullWordSet();

        // 3. 在原文本中查找所有“完整词”
        List<String> fullMatches = fullWords.stream()
                .filter(text::contains)
                .collect(Collectors.toList());

        // 4. 先合并成候选集合（保持插入顺序去重）
        LinkedHashSet<String> candidate = new LinkedHashSet<>();
        candidate.addAll(fullMatches);
        candidate.addAll(dfaList);

        // 5. 按长度从大到小排序，做“最长优先”去重：
        //    如果一个词已经被更长的词包含，就不再保留
        List<String> sorted = new ArrayList<>(candidate);
        sorted.sort((a, b) -> Integer.compare(b.length(), a.length())); // 长的在前

        List<String> result = new ArrayList<>();
        for (String w : sorted) {
            boolean isSub = false;
            for (String kept : result) {
                if (kept.contains(w)) {
                    isSub = true;
                    break;
                }
            }
            if (!isSub) {
                result.add(w);
            }
        }

        // 6. 如果你希望按文本中出现的顺序再排一下，也可以：
        result.sort(Comparator.comparingInt(text::indexOf));

        return result;
    }

}
