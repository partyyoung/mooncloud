//package net.mooncloud.conf;
//
//import java.util.ArrayList;
//import java.util.Arrays;
//import java.util.HashMap;
//import java.util.Iterator;
//import java.util.List;
//import java.util.Map;
//import java.util.Properties;
//
//import org.apache.commons.logging.Log;
//import org.apache.commons.logging.LogFactory;
//import org.apache.hadoop.util.StringUtils;
//
//import com.google.common.base.Preconditions;
//
//public class Configuration implements Iterable<Map.Entry<String, String>> {
//	private static final Log LOG = LogFactory.getLog(Configuration.class);
//
//	private Properties properties;
//	private Properties overlay;
//
//	/**
//	 * Get the value of the <code>name</code> property, <code>null</code> if no
//	 * such property exists. If the key is deprecated, it returns the value of
//	 * the first key which replaces the deprecated key and is not null
//	 * 
//	 * Values are processed for <a href="#VariableExpansion">variable
//	 * expansion</a> before being returned.
//	 * 
//	 * @param name
//	 *            the property name.
//	 * @return the value of the <code>name</code> or its replacing property, or
//	 *         null if no such property exists.
//	 */
//	public String get(String name) {
//		String[] names = handleDeprecation(deprecationContext.get(), name);
//		String result = null;
//		for (String n : names) {
//			result = substituteVars(getProps().getProperty(n));
//		}
//		return result;
//	}
//
//	/**
//	 * Get the value of the <code>name</code> property as a trimmed
//	 * <code>String</code>, <code>null</code> if no such property exists. If the
//	 * key is deprecated, it returns the value of the first key which replaces
//	 * the deprecated key and is not null
//	 * 
//	 * Values are processed for <a href="#VariableExpansion">variable
//	 * expansion</a> before being returned.
//	 * 
//	 * @param name
//	 *            the property name.
//	 * @return the value of the <code>name</code> or its replacing property, or
//	 *         null if no such property exists.
//	 */
//	public String getTrimmed(String name) {
//		String value = get(name);
//
//		if (null == value) {
//			return null;
//		} else {
//			return value.trim();
//		}
//	}
//
//	/**
//	 * Get the value of the <code>name</code> property as a trimmed
//	 * <code>String</code>, <code>defaultValue</code> if no such property
//	 * exists. See @{Configuration#getTrimmed} for more details.
//	 * 
//	 * @param name
//	 *            the property name.
//	 * @param defaultValue
//	 *            the property default value.
//	 * @return the value of the <code>name</code> or defaultValue if it is not
//	 *         set.
//	 */
//	public String getTrimmed(String name, String defaultValue) {
//		String ret = getTrimmed(name);
//		return ret == null ? defaultValue : ret;
//	}
//
//	/**
//	 * Get the value of the <code>name</code> property, without doing <a
//	 * href="#VariableExpansion">variable expansion</a>.If the key is
//	 * deprecated, it returns the value of the first key which replaces the
//	 * deprecated key and is not null.
//	 * 
//	 * @param name
//	 *            the property name.
//	 * @return the value of the <code>name</code> property or its replacing
//	 *         property and null if no such property exists.
//	 */
//	public String getRaw(String name) {
//		String[] names = handleDeprecation(deprecationContext.get(), name);
//		String result = null;
//		for (String n : names) {
//			result = getProps().getProperty(n);
//		}
//		return result;
//	}
//
//	/**
//	 * Returns alternative names (non-deprecated keys or previously-set
//	 * deprecated keys) for a given non-deprecated key. If the given key is
//	 * deprecated, return null.
//	 * 
//	 * @param name
//	 *            property name.
//	 * @return alternative names.
//	 */
//	private String[] getAlternativeNames(String name) {
//		String altNames[] = null;
//		DeprecatedKeyInfo keyInfo = null;
//		DeprecationContext cur = deprecationContext.get();
//		String depKey = cur.getReverseDeprecatedKeyMap().get(name);
//		if (depKey != null) {
//			keyInfo = cur.getDeprecatedKeyMap().get(depKey);
//			if (keyInfo.newKeys.length > 0) {
//				if (getProps().containsKey(depKey)) {
//					// if deprecated key is previously set explicitly
//					List<String> list = new ArrayList<String>();
//					list.addAll(Arrays.asList(keyInfo.newKeys));
//					list.add(depKey);
//					altNames = list.toArray(new String[list.size()]);
//				} else {
//					altNames = keyInfo.newKeys;
//				}
//			}
//		}
//		return altNames;
//	}
//
//	/**
//	 * Set the <code>value</code> of the <code>name</code> property. If
//	 * <code>name</code> is deprecated or there is a deprecated name associated
//	 * to it, it sets the value to both names.
//	 * 
//	 * @param name
//	 *            property name.
//	 * @param value
//	 *            property value.
//	 */
//	public void set(String name, String value) {
//		set(name, value, null);
//	}
//
//	/**
//	 * Set the <code>value</code> of the <code>name</code> property. If
//	 * <code>name</code> is deprecated, it also sets the <code>value</code> to
//	 * the keys that replace the deprecated key.
//	 * 
//	 * @param name
//	 *            property name.
//	 * @param value
//	 *            property value.
//	 * @param source
//	 *            the place that this configuration value came from (For
//	 *            debugging).
//	 * @throws IllegalArgumentException
//	 *             when the value or name is null.
//	 */
//	public void set(String name, String value, String source) {
//		Preconditions.checkArgument(name != null,
//				"Property name must not be null");
//		Preconditions.checkArgument(value != null, "The value of property "
//				+ name + " must not be null");
//		DeprecationContext deprecations = deprecationContext.get();
//		if (deprecations.getDeprecatedKeyMap().isEmpty()) {
//			getProps();
//		}
//		getOverlay().setProperty(name, value);
//		getProps().setProperty(name, value);
//		String newSource = (source == null ? "programatically" : source);
//
//		if (!isDeprecated(name)) {
//			updatingResource.put(name, new String[] { newSource });
//			String[] altNames = getAlternativeNames(name);
//			if (altNames != null) {
//				for (String n : altNames) {
//					if (!n.equals(name)) {
//						getOverlay().setProperty(n, value);
//						getProps().setProperty(n, value);
//						updatingResource.put(n, new String[] { newSource });
//					}
//				}
//			}
//		} else {
//			String[] names = handleDeprecation(deprecationContext.get(), name);
//			String altSource = "because " + name + " is deprecated";
//			for (String n : names) {
//				getOverlay().setProperty(n, value);
//				getProps().setProperty(n, value);
//				updatingResource.put(n, new String[] { altSource });
//			}
//		}
//	}
//
//	private void warnOnceIfDeprecated(DeprecationContext deprecations,
//			String name) {
//		DeprecatedKeyInfo keyInfo = deprecations.getDeprecatedKeyMap()
//				.get(name);
//		if (keyInfo != null && !keyInfo.getAndSetAccessed()) {
//			LOG_DEPRECATION.info(keyInfo.getWarningMessage(name));
//		}
//	}
//
//	/**
//	 * Unset a previously set property.
//	 */
//	public synchronized void unset(String name) {
//		String[] names = null;
//		if (!isDeprecated(name)) {
//			names = getAlternativeNames(name);
//			if (names == null) {
//				names = new String[] { name };
//			}
//		} else {
//			names = handleDeprecation(deprecationContext.get(), name);
//		}
//
//		for (String n : names) {
//			getOverlay().remove(n);
//			getProps().remove(n);
//		}
//	}
//
//	/**
//	 * Sets a property if it is currently unset.
//	 * 
//	 * @param name
//	 *            the property name
//	 * @param value
//	 *            the new value
//	 */
//	public synchronized void setIfUnset(String name, String value) {
//		if (get(name) == null) {
//			set(name, value);
//		}
//	}
//
//	private synchronized Properties getOverlay() {
//		if (overlay == null) {
//			overlay = new Properties();
//		}
//		return overlay;
//	}
//
//	/**
//	 * Get the value of the <code>name</code>. If the key is deprecated, it
//	 * returns the value of the first key which replaces the deprecated key and
//	 * is not null. If no such property exists, then <code>defaultValue</code>
//	 * is returned.
//	 * 
//	 * @param name
//	 *            property name.
//	 * @param defaultValue
//	 *            default value.
//	 * @return property value, or <code>defaultValue</code> if the property
//	 *         doesn't exist.
//	 */
//	public String get(String name, String defaultValue) {
//		return getProps().getProperty(name, defaultValue);
//	}
//
//	/**
//	 * Get the value of the <code>name</code> property as an <code>int</code>.
//	 * 
//	 * If no such property exists, the provided default value is returned, or if
//	 * the specified value is not a valid <code>int</code>, then an error is
//	 * thrown.
//	 * 
//	 * @param name
//	 *            property name.
//	 * @param defaultValue
//	 *            default value.
//	 * @throws NumberFormatException
//	 *             when the value is invalid
//	 * @return property value as an <code>int</code>, or
//	 *         <code>defaultValue</code>.
//	 */
//	public int getInt(String name, int defaultValue) {
//		String valueString = getTrimmed(name);
//		if (valueString == null)
//			return defaultValue;
//		String hexString = getHexDigits(valueString);
//		if (hexString != null) {
//			return Integer.parseInt(hexString, 16);
//		}
//		return Integer.parseInt(valueString);
//	}
//
//	/**
//	 * Get the comma delimited values of the <code>name</code> property as an
//	 * array of <code>String</code>s, trimmed of the leading and trailing
//	 * whitespace. If no such property is specified then an empty array is
//	 * returned.
//	 * 
//	 * @param name
//	 *            property name.
//	 * @return property value as an array of trimmed <code>String</code>s, or
//	 *         empty array.
//	 */
//	public String[] getTrimmedStrings(String name) {
//		String valueString = get(name);
//		return StringUtils.getTrimmedStrings(valueString);
//	}
//
//	/**
//	 * Get the value of the <code>name</code> property as a set of
//	 * comma-delimited <code>int</code> values.
//	 * 
//	 * If no such property exists, an empty array is returned.
//	 * 
//	 * @param name
//	 *            property name
//	 * @return property value interpreted as an array of comma-delimited
//	 *         <code>int</code> values
//	 */
//	public int[] getInts(String name) {
//		String[] strings = getTrimmedStrings(name);
//		int[] ints = new int[strings.length];
//		for (int i = 0; i < strings.length; i++) {
//			ints[i] = Integer.parseInt(strings[i]);
//		}
//		return ints;
//	}
//
//	/**
//	 * Set the value of the <code>name</code> property to an <code>int</code>.
//	 * 
//	 * @param name
//	 *            property name.
//	 * @param value
//	 *            <code>int</code> value of the property.
//	 */
//	public void setInt(String name, int value) {
//		set(name, Integer.toString(value));
//	}
//
//	/**
//	 * Get the value of the <code>name</code> property as a <code>long</code>.
//	 * If no such property exists, the provided default value is returned, or if
//	 * the specified value is not a valid <code>long</code>, then an error is
//	 * thrown.
//	 * 
//	 * @param name
//	 *            property name.
//	 * @param defaultValue
//	 *            default value.
//	 * @throws NumberFormatException
//	 *             when the value is invalid
//	 * @return property value as a <code>long</code>, or
//	 *         <code>defaultValue</code>.
//	 */
//	public long getLong(String name, long defaultValue) {
//		String valueString = getTrimmed(name);
//		if (valueString == null)
//			return defaultValue;
//		String hexString = getHexDigits(valueString);
//		if (hexString != null) {
//			return Long.parseLong(hexString, 16);
//		}
//		return Long.parseLong(valueString);
//	}
//
//	/**
//	 * Get the value of the <code>name</code> property as a <code>long</code> or
//	 * human readable format. If no such property exists, the provided default
//	 * value is returned, or if the specified value is not a valid
//	 * <code>long</code> or human readable format, then an error is thrown. You
//	 * can use the following suffix (case insensitive): k(kilo), m(mega),
//	 * g(giga), t(tera), p(peta), e(exa)
//	 * 
//	 * @param name
//	 *            property name.
//	 * @param defaultValue
//	 *            default value.
//	 * @throws NumberFormatException
//	 *             when the value is invalid
//	 * @return property value as a <code>long</code>, or
//	 *         <code>defaultValue</code>.
//	 */
//	public long getLongBytes(String name, long defaultValue) {
//		String valueString = getTrimmed(name);
//		if (valueString == null)
//			return defaultValue;
//		return StringUtils.TraditionalBinaryPrefix.string2long(valueString);
//	}
//
//	private String getHexDigits(String value) {
//		boolean negative = false;
//		String str = value;
//		String hexString = null;
//		if (value.startsWith("-")) {
//			negative = true;
//			str = value.substring(1);
//		}
//		if (str.startsWith("0x") || str.startsWith("0X")) {
//			hexString = str.substring(2);
//			if (negative) {
//				hexString = "-" + hexString;
//			}
//			return hexString;
//		}
//		return null;
//	}
//
//	/**
//	 * Set the value of the <code>name</code> property to a <code>long</code>.
//	 * 
//	 * @param name
//	 *            property name.
//	 * @param value
//	 *            <code>long</code> value of the property.
//	 */
//	public void setLong(String name, long value) {
//		set(name, Long.toString(value));
//	}
//
//	/**
//	 * Get the value of the <code>name</code> property as a <code>float</code>.
//	 * If no such property exists, the provided default value is returned, or if
//	 * the specified value is not a valid <code>float</code>, then an error is
//	 * thrown.
//	 * 
//	 * @param name
//	 *            property name.
//	 * @param defaultValue
//	 *            default value.
//	 * @throws NumberFormatException
//	 *             when the value is invalid
//	 * @return property value as a <code>float</code>, or
//	 *         <code>defaultValue</code>.
//	 */
//	public float getFloat(String name, float defaultValue) {
//		String valueString = getTrimmed(name);
//		if (valueString == null)
//			return defaultValue;
//		return Float.parseFloat(valueString);
//	}
//
//	/**
//	 * Set the value of the <code>name</code> property to a <code>float</code>.
//	 * 
//	 * @param name
//	 *            property name.
//	 * @param value
//	 *            property value.
//	 */
//	public void setFloat(String name, float value) {
//		set(name, Float.toString(value));
//	}
//
//	/**
//	 * Get the value of the <code>name</code> property as a <code>double</code>.
//	 * If no such property exists, the provided default value is returned, or if
//	 * the specified value is not a valid <code>double</code>, then an error is
//	 * thrown.
//	 * 
//	 * @param name
//	 *            property name.
//	 * @param defaultValue
//	 *            default value.
//	 * @throws NumberFormatException
//	 *             when the value is invalid
//	 * @return property value as a <code>double</code>, or
//	 *         <code>defaultValue</code>.
//	 */
//	public double getDouble(String name, double defaultValue) {
//		String valueString = getTrimmed(name);
//		if (valueString == null)
//			return defaultValue;
//		return Double.parseDouble(valueString);
//	}
//
//	/**
//	 * Set the value of the <code>name</code> property to a <code>double</code>.
//	 * 
//	 * @param name
//	 *            property name.
//	 * @param value
//	 *            property value.
//	 */
//	public void setDouble(String name, double value) {
//		set(name, Double.toString(value));
//	}
//
//	/**
//	 * Get the value of the <code>name</code> property as a <code>boolean</code>
//	 * . If no such property is specified, or if the specified value is not a
//	 * valid <code>boolean</code>, then <code>defaultValue</code> is returned.
//	 * 
//	 * @param name
//	 *            property name.
//	 * @param defaultValue
//	 *            default value.
//	 * @return property value as a <code>boolean</code>, or
//	 *         <code>defaultValue</code>.
//	 */
//	public boolean getBoolean(String name, boolean defaultValue) {
//		String valueString = getTrimmed(name);
//		if (null == valueString || valueString.isEmpty()) {
//			return defaultValue;
//		}
//
//		valueString = valueString.toLowerCase();
//
//		if ("true".equals(valueString))
//			return true;
//		else if ("false".equals(valueString))
//			return false;
//		else
//			return defaultValue;
//	}
//
//	/**
//	 * Set the value of the <code>name</code> property to a <code>boolean</code>
//	 * .
//	 * 
//	 * @param name
//	 *            property name.
//	 * @param value
//	 *            <code>boolean</code> value of the property.
//	 */
//	public void setBoolean(String name, boolean value) {
//		set(name, Boolean.toString(value));
//	}
//
//	protected synchronized Properties getProps() {
//		if (properties == null) {
//			properties = new Properties();
//		}
//		return properties;
//	}
//
//	/**
//	 * Get an {@link Iterator} to go through the list of <code>String</code>
//	 * key-value pairs in the configuration.
//	 * 
//	 * @return an iterator over the entries.
//	 */
//	@Override
//	public Iterator<Map.Entry<String, String>> iterator() {
//		// Get a copy of just the string to string pairs. After the old object
//		// methods that allow non-strings to be put into configurations are
//		// removed,
//		// we could replace properties with a Map<String,String> and get rid of
//		// this
//		// code.
//		Map<String, String> result = new HashMap<String, String>();
//		for (Map.Entry<Object, Object> item : getProps().entrySet()) {
//			if (item.getKey() instanceof String
//					&& item.getValue() instanceof String) {
//				result.put((String) item.getKey(), (String) item.getValue());
//			}
//		}
//		return result.entrySet().iterator();
//	}
//
//}
