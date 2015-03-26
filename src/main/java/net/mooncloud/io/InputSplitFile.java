package net.mooncloud.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class InputSplitFile implements WritableComparable<InputSplitFile>
{

	private long offset;
	private String fileName;
	private String parentName;
	private String grandparentName;
	private String dt;
	private String tableName;
	private Path filePath;

	public long getOffset()
	{
		return offset;
	}

	public void setOffset(long offset)
	{
		this.offset = offset;
	}

	public String getFileName()
	{
		return fileName;
	}

	public void setFileName(String fileName)
	{
		this.fileName = fileName;
	}

	public String getParentName()
	{
		return parentName;
	}

	public void setParentName(String parentName)
	{
		this.parentName = parentName;
	}

	public String getGrandparentName()
	{
		return grandparentName;
	}

	public void setGrandparentName(String grandparentName)
	{
		this.grandparentName = grandparentName;
	}

	public String getDt()
	{
		return dt;
	}

	public void setDt(String dt)
	{
		this.dt = dt;
		this.parentName = dt;
	}

	public String getTableName()
	{
		return tableName;
	}

	public void setTableName(String tableName)
	{
		this.tableName = tableName;
		this.grandparentName = tableName;
	}

	public Path getFilePath()
	{
		return filePath;
	}

	public void setFilePath(Path filePath)
	{
		this.filePath = filePath;
	}

	public void readFields(DataInput in) throws IOException
	{
		this.offset = in.readLong();
		this.fileName = Text.readString(in);
	}

	public void write(DataOutput out) throws IOException
	{
		out.writeLong(offset);
		Text.writeString(out, fileName);
	}

	public int compareTo(InputSplitFile o)
	{
		InputSplitFile that = (InputSplitFile) o;

		int f = this.fileName.compareTo(that.fileName);
		if (f == 0)
		{
			return (int) Math.signum((double) (this.offset - that.offset));
		}
		return f;
	}

	public boolean equals(InputSplitFile obj)
	{
		if (obj instanceof InputSplitFile)
			return this.compareTo(obj) == 0;
		return false;
	}

	@Override
	public int hashCode()
	{
		assert false : "hashCode not designed";
		return 42; // an arbitrary constant
	}
}
