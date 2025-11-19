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


        String word = "共铲挡 wo 我热爱祖国，这个lulu的衣服真不错，臭狗屎，垃圾 草尼玛 草你妈的 鉴别处男 遥控色子,五星红旗迎风飘扬，毛主席的画像屹立在天安门前";
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

        /*TextAnalyzeService.AnalyzeResult p0 = TextAnalyzeService.analyzeP0(word);
        TextAnalyzeService.AnalyzeResult p1 = TextAnalyzeService.analyzeP1(word);

        System.out.println("P0 命中：" + p0.sensitiveWords);
        System.out.println("P1 命中：" + p1.sensitiveWords);*/


        System.err.println(MD5Hash.getMD5AsHex("26387e18-649c-47d7-a28f-ef5aa728c08f".getBytes()));


    }
}
