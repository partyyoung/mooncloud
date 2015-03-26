package net.mooncloud.util;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DecodeURL {
	static URLDecoder decode = new URLDecoder();
	private static Map encodeMap = new HashMap<String, String>();
	static {
			encodeMap.put("0", "%30");
			encodeMap.put("1", "%31");
			encodeMap.put("2", "%32");
			encodeMap.put("3", "%33");
			encodeMap.put("4", "%34");
			encodeMap.put("5", "%35");
			encodeMap.put("6", "%36");
			encodeMap.put("7", "%37");
			encodeMap.put("8", "%38");
			encodeMap.put("9", "%39");
			encodeMap.put("A", "%41");
			encodeMap.put("B", "%42");
			encodeMap.put("C", "%43");
			encodeMap.put("D", "%44");
			encodeMap.put("E", "%45");
			encodeMap.put("F", "%46");
			encodeMap.put("G", "%47");
			encodeMap.put("H", "%48");
			encodeMap.put("I", "%49");
			encodeMap.put("J", "%4A");
			encodeMap.put("K", "%4B");
			encodeMap.put("L", "%4C");
			encodeMap.put("M", "%4D");
			encodeMap.put("N", "%4E");
			encodeMap.put("O", "%4F");
			encodeMap.put("P", "%50");
			encodeMap.put("Q", "%51");
			encodeMap.put("R", "%52");
			encodeMap.put("S", "%53");
			encodeMap.put("T", "%54");
			encodeMap.put("U", "%55");
			encodeMap.put("V", "%56");
			encodeMap.put("W", "%57");
			encodeMap.put("X", "%58");
			encodeMap.put("Y", "%59");
			encodeMap.put("Z", "%5A");
			encodeMap.put("a", "%61");
			encodeMap.put("b", "%62");
			encodeMap.put("c", "%63");
			encodeMap.put("d", "%64");
			encodeMap.put("e", "%65");
			encodeMap.put("f", "%66");
			encodeMap.put("g", "%67");
			encodeMap.put("h", "%68");
			encodeMap.put("i", "%69");
			encodeMap.put("j", "%6A");
			encodeMap.put("k", "%6B");
			encodeMap.put("l", "%6C");
			encodeMap.put("m", "%6D");
			encodeMap.put("n", "%6E");
			encodeMap.put("o", "%6F");
			encodeMap.put("p", "%70");
			encodeMap.put("q", "%71");
			encodeMap.put("r", "%72");
			encodeMap.put("s", "%73");
			encodeMap.put("t", "%74");
			encodeMap.put("u", "%75");
			encodeMap.put("v", "%76");
			encodeMap.put("w", "%77");
			encodeMap.put("x", "%78");
			encodeMap.put("y", "%79");
			encodeMap.put("z", "%7A");
	}

	public static String decode(String undecode) {

		String str = UnicodeToStr.decode(undecode.replaceAll("\\%u", "\\\\u"));
		Pattern pattern;
		Matcher matcher;
		String encode = "gbk";
		pattern = Pattern.compile("\\%25");
		do {
			str = str.replaceAll("\\%25", "\\%");
			matcher = pattern.matcher(str);
		} while (matcher.find());
		Pattern p = Pattern.compile("(\\%\\p{XDigit}{2})+([0-9a-zA-Z])");
		Matcher m = p.matcher(str);
		while (m.find()) {
			str = str.replaceFirst(
					"((\\%\\p{XDigit}{2})+)(" + m.group(2) + ")", "$1"
							+ encodeMap.get(m.group(2)));
		}
		pattern = Pattern.compile("((\\%\\p{XDigit}{2})+)");
		matcher = pattern.matcher(str);
		while (matcher.find()) {
			String old = matcher.group(1);
			try {
				matcher = Pattern.compile(
						"^([\\x00-\\x7f]|[\\xe0-\\xef][\\x80-\\xbf]{2})+$")
						.matcher(decode.decode(old, "iso-8859-1"));
				if (matcher.find()) {
					encode = "utf-8";
				} else {
					encode = "gbk";
				}
			} catch (UnsupportedEncodingException e1) {
				e1.printStackTrace();
			}
			String result;
			try {
				result = decode.decode(old, encode);
				str = str.replace(old, result);
				matcher = pattern.matcher(str);
			} catch (UnsupportedEncodingException e) {
				e.printStackTrace();
			}
		}
		return str;
	}
}
