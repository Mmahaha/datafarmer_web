package kd.fi.gl.datafarmer.core.util;

import java.util.StringJoiner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 字符串高效工具类
 */
public class FastStringUtils {

    /**
     * 对指定的数字，生成多个前导0
     *
     * @param index       索引值
     * @param totalLength 结果总长度
     * @return 形如 00000123 的字符串
     */
    public static String addLeadingZeros(int index, int totalLength) {
        String indexStr = String.valueOf(index);
        int toAdd = totalLength - indexStr.length();
        StringBuilder stringBuilder = new StringBuilder();
        for (int i = 0; i < toAdd; i++) {
            stringBuilder.append("0");
        }
        stringBuilder.append(indexStr);
        return stringBuilder.toString();
    }

    public static String join(String delimiter, long[] array) {
        StringJoiner stringJoiner = new StringJoiner(delimiter);
        for (long value : array) {
            stringJoiner.add(String.valueOf(value));
        }
        return stringJoiner.toString();
    }

    /**
     * 从limit x (offset y) 中提取出x
     */
    public static int extractLimitFromLimitOffsetStr(String limitOffsetStr) {
        Pattern regex = Pattern.compile("limit\\s*(\\d+)");
        Matcher matcher = regex.matcher(limitOffsetStr);

        if (matcher.find()) {
            return Integer.parseInt(matcher.group(1));
        } else {
            throw new IllegalArgumentException("limitOffSetStr:" + limitOffsetStr);
        }
    }

}
