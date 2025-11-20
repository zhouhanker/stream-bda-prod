package com.stream.realtime.lululemon;

import com.github.houbb.sensitive.word.core.SensitiveWordHelper;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.stream.core.DateTimeUtils;
import com.stream.realtime.lululemon.service.TextAnalyzeService;
import com.stream.realtime.lululemon.utils.SensitiveWordUtils;
import lombok.SneakyThrows;
import org.apache.hadoop.hbase.util.MD5Hash;
import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;
import org.wltea.analyzer.lucene.IKAnalyzer;

import java.io.StringReader;
import java.util.Date;


/**
 * @Package com.stream.realtime.lululemon.Test
 * @Author zhou.han
 * @Date 2025/10/27 13:47
 * @description:
 */
public class Test {

    @SneakyThrows
    public static void main(String[] args) {

//        String s = "{\"log_id\":\"d664dbd910f44ab29f428bb061726b55\",\"device\":{\"brand\":\"honor\",\"plat\":\"android\",\"platv\":\"10\",\"softv\":\"7.84.0\",\"uname\":\"\",\"userkey\":\"fc9d23c817271c8e\",\"device\":\"bkl-al00\"},\"gis\":{\"ip\":\"120.235.32.180\"},\"network\":{\"net\":\"wifi\"},\"opa\":\"pageinfo\",\"log_type\":\"product_detail\",\"ts\":1761944836,\"product_id\":\"10109988265079\",\"order_id\":\"710c4b1920664c7fb959d76eaa5a6ba9\",\"user_id\":\"d717278c-9d31-48ac-bdb6-148e8254f048\"}";
//        JsonObject jsonObject = JsonParser.parseString(s).getAsJsonObject();
//        System.err.println(jsonObject);
//        long ts = jsonObject.get("ts").getAsLong();
//        System.err.println(ts);
//        System.err.println(DateTimeUtils.tsToDate(ts));


        String word = "{\\n\\n    \\\"用户评论\\\": \\\"LULulemon的男士运动裤真的烂到家了！穿上去感觉像在狗窝里待了一天！颜色也难看，洗完就掉色，腿型一点都没有改善，还是原来那副土ransparent look。好_ops，我投诉你们！\\\",\\n\\n    \\\"差评\\\": \\\"这款男士运动裤真是给我带来了无尽的困扰！\\\",\\n\\n    \\\"_attack\\\": \\\"LULulemon的男士运动裤质量差得离谱，颜色也很难看，穿上去就像穿着狗屎一样\\\",\\n\\n    \\\"不文明用语\\\": \\\"狗屎一样的颜色！洗完就掉色，腿型一点都没有改善，还是原来那副土ransparent look。好_ops，我投诉你们！\\\"\\n}";
        /*StringReader stringReader = new StringReader(word);
        IKSegmenter ikSegmenter = new IKSegmenter(stringReader, true);
        Lexeme lexeme;

        while ((lexeme = ikSegmenter.next()) != null){
            System.out.println(lexeme.getLexemeText());
        }

        System.err.println("=======================================================");
        for (String s : SensitiveWordHelper.findAll(word)) {
            System.err.println(s);
        }*/

        TextAnalyzeService.AnalyzeResult p0 = TextAnalyzeService.analyzeP0(word);
        TextAnalyzeService.AnalyzeResult p1 = TextAnalyzeService.analyzeP1(word);

        System.out.println("P0 命中：" + p0.sensitiveWords);
        System.out.println("P1 命中：" + p1.sensitiveWords);


//        System.err.println(MD5Hash.getMD5AsHex("ba0f9f0e-4fd9-470d-a034-e33cc1390ec9".getBytes()));


    }
}
