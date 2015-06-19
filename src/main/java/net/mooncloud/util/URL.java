package net.mooncloud.util;

import java.io.Serializable;
import java.net.MalformedURLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public final class URL implements Serializable
{

	private static final long serialVersionUID = -1985165475234910535L;

	private final String protocol;

	private final String username;

	private final String password;

	private final String host;

	private final int port;

	private final String path;

	private final Map<String, String> parameters;

	// ==== cache ====

	private volatile transient String string;

	protected URL()
	{
		this.protocol = null;
		this.username = null;
		this.password = null;
		this.host = null;
		this.port = 0;
		this.path = null;
		this.parameters = null;
	}

	public URL(String protocol, String username, String password, String host, int port, String path, Map<String, String> parameters)
	{
		if ((username == null || username.length() == 0) && password != null && password.length() > 0)
		{
			throw new IllegalArgumentException("Invalid url, password without username!");
		}
		this.protocol = protocol;
		this.username = username;
		this.password = password;
		this.host = host;
		this.port = (port < 0 ? 0 : port);
		this.path = path;
		// trim the beginning "/"
		while (path != null && path.startsWith("/"))
		{
			path = path.substring(1);
		}
		if (parameters == null)
		{
			parameters = new HashMap<String, String>();
		}
		else
		{
			parameters = new HashMap<String, String>(parameters);
		}
		this.parameters = Collections.unmodifiableMap(parameters);
	}

	/**
	 * Parse url string
	 * 
	 * @param url
	 *            URL string
	 * @return URL instance
	 * @see URL
	 */
	public static URL valueOf(String url)
	{
		if (url == null || (url = url.trim()).length() == 0)
		{
			throw new IllegalArgumentException("url == null");
		}
		String protocol = null;
		String username = null;
		String password = null;
		String host = null;
		int port = 0;
		String path = null;
		Map<String, String> parameters = null;
		int i = url.indexOf("?"); // seperator between body and parameters
		if (i >= 0)
		{
			String[] parts = url.substring(i + 1).split("\\&");
			parameters = new HashMap<String, String>();
			for (String part : parts)
			{
				part = part.trim();
				if (part.length() > 0)
				{
					int j = part.indexOf('=');
					if (j >= 0)
					{
						parameters.put(part.substring(0, j), part.substring(j + 1));
					}
					else
					{
						parameters.put(part, part);
					}
				}
			}
			url = url.substring(0, i);
		}
		i = url.indexOf("://");
		if (i >= 0)
		{
			if (i == 0)
				throw new IllegalStateException("url missing protocol: \"" + url + "\"");
			protocol = url.substring(0, i);
			url = url.substring(i + 3);
		}
		else
		{
			// case: file:/path/to/file.txt
			i = url.indexOf(":/");
			if (i >= 0)
			{
				if (i == 0)
					throw new IllegalStateException("url missing protocol: \"" + url + "\"");
				protocol = url.substring(0, i);
				url = url.substring(i + 1);
			}
		}

		i = url.indexOf("/");
		if (i >= 0)
		{
			path = url.substring(i + 1);
			url = url.substring(0, i);
		}
		i = url.indexOf("@");
		if (i >= 0)
		{
			username = url.substring(0, i);
			int j = username.indexOf(":");
			if (j >= 0)
			{
				password = username.substring(j + 1);
				username = username.substring(0, j);
			}
			url = url.substring(i + 1);
		}
		i = url.indexOf(":");
		if (i >= 0 && i < url.length() - 1)
		{
			port = Integer.parseInt(url.substring(i + 1));
			url = url.substring(0, i);
		}
		if (url.length() > 0)
			host = url;
		return new URL(protocol, username, password, host, port, path, parameters);
	}

	public String getProtocol()
	{
		return protocol;
	}

	public String getUsername()
	{
		return username;
	}

	public String getPassword()
	{
		return password;
	}

	public String getHost()
	{
		return host;
	}

	public int getPort()
	{
		return port;
	}

	public String getPath()
	{
		return path;
	}

	public Map<String, String> getParameters()
	{
		return parameters;
	}

	@Override
	public String toString()
	{
		if (string != null)
		{
			return string;
		}
		return string = buildString(false, true); // no show username and
													// password
	}

	private String buildString(boolean appendUser, boolean appendParameter, String... parameters)
	{
		StringBuilder buf = new StringBuilder();
		if (protocol != null && protocol.length() > 0)
		{
			buf.append(protocol);
			buf.append("://");
		}
		if (appendUser && username != null && username.length() > 0)
		{
			buf.append(username);
			if (password != null && password.length() > 0)
			{
				buf.append(":");
				buf.append(password);
			}
			buf.append("@");
		}
		String host;
		host = getHost();
		if (host != null && host.length() > 0)
		{
			buf.append(host);
			if (port > 0)
			{
				buf.append(":");
				buf.append(port);
			}
		}
		String path;
		path = getPath();
		if (path != null && path.length() > 0)
		{
			buf.append("/");
			buf.append(path);
		}
		if (appendParameter)
		{
			buildParameters(buf, true, parameters);
		}
		return buf.toString();
	}

	private void buildParameters(StringBuilder buf, boolean concat, String[] parameters)
	{
		if (getParameters() != null && getParameters().size() > 0)
		{
			List<String> includes = (parameters == null || parameters.length == 0 ? null : Arrays.asList(parameters));
			boolean first = true;
			for (Map.Entry<String, String> entry : new TreeMap<String, String>(getParameters()).entrySet())
			{
				if (entry.getKey() != null && entry.getKey().length() > 0 && (includes == null || includes.contains(entry.getKey())))
				{
					if (first)
					{
						if (concat)
						{
							buf.append("?");
						}
						first = false;
					}
					else
					{
						buf.append("&");
					}
					buf.append(entry.getKey());
					buf.append("=");
					buf.append(entry.getValue() == null ? "" : entry.getValue().trim());
				}
			}
		}
	}

	public java.net.URL toJavaURL()
	{
		try
		{
			return new java.net.URL(toString());
		}
		catch (MalformedURLException e)
		{
			throw new IllegalStateException(e.getMessage(), e);
		}
	}

	@Override
	public int hashCode()
	{
		final int prime = 31;
		int result = 1;
		result = prime * result + ((host == null) ? 0 : host.hashCode());
		result = prime * result + ((parameters == null) ? 0 : parameters.hashCode());
		result = prime * result + ((password == null) ? 0 : password.hashCode());
		result = prime * result + ((path == null) ? 0 : path.hashCode());
		result = prime * result + port;
		result = prime * result + ((protocol == null) ? 0 : protocol.hashCode());
		result = prime * result + ((username == null) ? 0 : username.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj)
	{
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		URL other = (URL) obj;
		if (host == null)
		{
			if (other.host != null)
				return false;
		}
		else if (!host.equals(other.host))
			return false;
		if (parameters == null)
		{
			if (other.parameters != null)
				return false;
		}
		else if (!parameters.equals(other.parameters))
			return false;
		if (password == null)
		{
			if (other.password != null)
				return false;
		}
		else if (!password.equals(other.password))
			return false;
		if (path == null)
		{
			if (other.path != null)
				return false;
		}
		else if (!path.equals(other.path))
			return false;
		if (port != other.port)
			return false;
		if (protocol == null)
		{
			if (other.protocol != null)
				return false;
		}
		else if (!protocol.equals(other.protocol))
			return false;
		if (username == null)
		{
			if (other.username != null)
				return false;
		}
		else if (!username.equals(other.username))
			return false;
		return true;
	}
}
