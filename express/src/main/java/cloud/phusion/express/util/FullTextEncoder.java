package cloud.phusion.express.util;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * For naive full-text searching in database without applying advanced tokenization mechanisms.
 */
public class FullTextEncoder {

    public static String encode(String str) {
        StringBuilder result = new StringBuilder();
        int len = str.length();

        for (int i = 0; i < len; i++) {
            char c = str.charAt(i);
            if (c < 2048) result.append(c);
            else result.append('{').append((int)c).append('}');
        }

        return result.toString();
    }

    private static Pattern patternChar = Pattern.compile("\\{[0-9]{5}\\}");

    public static String decode(String str) {
        Matcher matcher = patternChar.matcher(str);

        StringBuilder result = new StringBuilder();
        int pos = 0;

        while (matcher.find()) {
            if (matcher.start() > pos) result.append(str.substring(pos, matcher.start()));

            result.append((char) Integer.parseInt( str.substring(matcher.start()+1,matcher.end()-1) ) );

            pos = matcher.end();
        }
        if (pos < str.length()) result.append(str.substring(pos));

        return result.toString();
    }

    private static Pattern patternDelimiter = Pattern.compile("[- ,_.]+");

    public static String encodeAsQueryString(String str) {
        Matcher matcher = patternDelimiter.matcher(str);

        StringBuilder result = new StringBuilder();
        int pos = 0;

        while (matcher.find()) {
            if (matcher.start() > pos) _encodeStringForQuery(result, str.substring(pos, matcher.start()));
            pos = matcher.end();
        }
        if (pos < str.length()) _encodeStringForQuery(result, str.substring(pos));

        return result.toString();
    }

    private static void _encodeStringForQuery(StringBuilder result, String str) {
        if (result.length() > 0) result.append(' ');
        int len = str.length();
        boolean isFirst = true;
        boolean isNewPart = true;

        for (int i = 0; i < len; i++) {
            char c = str.charAt(i);

            if (c < 2048) {
                if (isNewPart) {
                    if (isFirst) isFirst = false;
                    else result.append(' ');

                    result.append('+');
                    isNewPart = false;
                }
                result.append(c);
            }
            else {
                if (isFirst) isFirst = false;
                else result.append(' ');

                result.append('+').append((int)c);
                isNewPart = true;
            }
        }
    }

}
