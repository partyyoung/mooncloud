package net.mooncloud.util;

import java.text.DecimalFormat;
import java.text.SimpleDateFormat;

public class SomeStaticUtils
{
	public final static SimpleDateFormat DATEFORMAT1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	public final static SimpleDateFormat DATEFORMAT2 = new SimpleDateFormat("yyyy-MM-dd HH");
	public final static SimpleDateFormat DATEFORMAT3 = new SimpleDateFormat("yyyyMMdd");
	public final static SimpleDateFormat DATEFORMAT4 = new SimpleDateFormat("yyyy-MM-dd");

	public final static DecimalFormat df = new DecimalFormat("#.00");

	// public static final Gson gson = new GsonBuilder().disableHtmlEscaping()
	// .setLongSerializationPolicy(LongSerializationPolicy.STRING)
	// .create();
}
