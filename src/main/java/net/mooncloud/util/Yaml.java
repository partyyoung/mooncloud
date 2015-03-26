package net.mooncloud.util;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

public class Yaml extends org.yaml.snakeyaml.Yaml
{
	private static org.yaml.snakeyaml.Yaml o = new org.yaml.snakeyaml.Yaml();

	public Yaml()
	{
		// o = new org.yaml.snakeyaml.Yaml();
	}

	/**
	 * Serialize a Java object into a YAML Flat String.
	 * 
	 * @param data
	 *            Java object to be Serialized to YAML
	 * @return YAML String
	 */
	public static String dumpFlat(Object data)
	{
		return o.dump(data).replace("\n", "\\n");
	}

	/**
	 * Parse the only YAML document in a String and produce the corresponding
	 * Java object. (Because the encoding in known BOM is not respected.)
	 * 
	 * @param yaml
	 *            YAML data to load from (BOM must not be present)
	 * @return parsed object
	 */
	public static Object loadFlat(String yaml)
	{
		return o.load(yaml.replace("\\n", "\n"));
	}

	public static Object loadResources(String yamlfile)
	{
		// URL url = Yaml.class.getClassLoader().getResource(yamlfile);
		// URL url = ClassLoader.getSystemResource(confName);
		InputStream is = Yaml.class.getClassLoader().getResourceAsStream(yamlfile);
		// InputStream is = ClassLoader.getSystemResourceAsStream(confName);
		InputStreamReader isr = new InputStreamReader(is, Charset.forName("UTF-8"));
		return o.load(isr);
	}

	public static void main(String[] args) throws Exception
	{
		ArrayList ruleList = new ArrayList();

		HashMap rule1 = new HashMap();
		ArrayList rule1Value1 = new ArrayList();
		HashMap rule1Value1Filter1 = new HashMap();
		ArrayList rule1Value1Filter1Value = new ArrayList();
		rule1Value1Filter1Value.add(".jd.");
		rule1Value1Filter1Value.add("activity");
		rule1Value1Filter1.put("contains", rule1Value1Filter1Value);

		HashMap rule1Value1Filter2 = new HashMap();
		ArrayList rule1Value1Filter2Value = new ArrayList();
		rule1Value1Filter2Value.add("aaa");
		rule1Value1Filter2Value.add("bbb");
		rule1Value1Filter2.put("-contains", rule1Value1Filter2Value);
		rule1Value1Filter2.put("endsWith", ".taobao.com");

		rule1Value1.add(rule1Value1Filter1);
		rule1Value1.add(rule1Value1Filter2);
		rule1.put("getStaticAccessUrl", rule1Value1);
		ruleList.add(rule1);

		HashMap rule2 = new HashMap();
		// ArrayList rule2Value1 = new ArrayList();
		// HashMap rule2Value1Filter1 = new HashMap();
		// ArrayList rule2Value1Filter1Value = new ArrayList();
		// rule2Value1Filter1Value.add(".jd.");
		// rule2Value1Filter1Value.add("activity");
		// rule2Value1Filter1.put("contains", rule2Value1Filter1Value);

		HashMap rule2Value1Filter2 = new HashMap();
		ArrayList rule2Value1Filter2Value = new ArrayList();
		rule2Value1Filter2Value.add("aaa");
		rule2Value1Filter2Value.add("bbb");
		rule2Value1Filter2.put("contains", rule2Value1Filter2Value);
		// rule2Value1Filter2.put("endsWith", null);

		// rule2Value1.add(rule2Value1Filter1);
		// rule2Value1.add(rule2Value1Filter2);
		rule2.put("getStaticReferUrl", rule2Value1Filter2);
		ruleList.add(rule2);

		HashMap<String, Object> dmpidRules = new HashMap<String, Object>();
		dmpidRules.put("id1", ruleList);

		Yaml y = new Yaml();
		String dump = y.dump(dmpidRules);
		System.out.println(dump);
		Object l = y.load(dump);
		System.out.println(l.getClass());

	}
}
