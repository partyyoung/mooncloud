package net.mooncloud;

public class FieldSchema
{
	public final String name;
	public final String type;

	public FieldSchema(String arg0, String arg1)
	{
		if ("text".equals(arg1))
		{
			arg1 = "string";
		}
		else if ("bigint".equals(arg1))
		{
			arg1 = "long";
		}
		name = arg0;
		type = arg1;
	}

	public String toString()
	{
		StringBuilder dup = new StringBuilder(name);
		dup.append(":").append(type);
		return dup.toString();
	}
	
}
