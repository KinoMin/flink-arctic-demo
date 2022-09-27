package com.kino.arctic.arcticproduce;

import java.text.DecimalFormat;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Utils {

    public static final int MAX = 1000000;
    public static final int MIN = 1;
    private static Pattern UNDERLINE_PATTERN = Pattern.compile("_([a-z])");


    /**
     * 获取一个随机数(整数)
     * @return
     */
    public static final int getRandomTime() {
        Random random = new Random();
        return (random.nextInt(MAX) % (MAX - MIN + 1) + MIN);
    }

    public static final double getRandomDouble(){
        return Double.parseDouble(new DecimalFormat("###.000").format(Math.random()*100));
    }

    /**
     * 根据传入的带下划线的字符串转化为驼峰格式
     * @param str
     * @author mrf
     * @return
     */

    public static String underlineToHump(String str){
        //正则匹配下划线及后一个字符，删除下划线并将匹配的字符转成大写
        Matcher matcher = UNDERLINE_PATTERN.matcher(str);
        StringBuffer sb = new StringBuffer(str);
        if (matcher.find()) {
            sb = new StringBuffer();
            //将当前匹配的子串替换成指定字符串，并且将替换后的子串及之前到上次匹配的子串之后的字符串添加到StringBuffer对象中
            //正则之前的字符和被替换的字符
            matcher.appendReplacement(sb, matcher.group(1).toUpperCase());
            //把之后的字符串也添加到StringBuffer对象中
            matcher.appendTail(sb);
        } else {
            //去除除字母之外的前面带的下划线
            return sb.toString().replaceAll("_", "");
        }
        return underlineToHump(sb.toString());
    }

}
