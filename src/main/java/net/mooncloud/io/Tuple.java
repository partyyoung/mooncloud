package net.mooncloud.io;

import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.ObjectOutputStream;

import org.apache.hadoop.io.BinaryComparable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;

/**
 * 元组，继承了WritableComparable，可作为key
 * 
 * @author yangjd
 * 
 * @see org.apache.hadoop.io.WritableComparable
 * @see org.apache.hadoop.mapred.join.TupleWritable
 * 
 */
public class Tuple extends BinaryComparable implements WritableComparable<BinaryComparable>
{
//	private static final Log LOG = LogFactory.getLog(Tuple.class);
	private Writable[] values;
	private int size;
	public Tuple()
	{
		values = new Writable[1];
		size = 1;
	}

	public Tuple(int n)
	{
		values = new Writable[n];
		size = n;
	}
	
	/**
	  * Get ith Writable from Tuple.
	  */
	public Writable get(int i)
	{
		if (i < 0 || i >= size)
		{
			throw new ArrayIndexOutOfBoundsException(i);
		}
		return values[i];
	}

	/**
	  * Set ith Writable to Tuple.
	  */
	public Writable set(int i, Writable w)
	{
		if (i < 0 || i >= size)
		{
			throw new ArrayIndexOutOfBoundsException(i);
		}
//		classes[i] = w.getClass();
		return values[i] = w;
	}
	
	 /** Writes each Writable to <code>out</code>.
	   * TupleWritable format:
	   * {@code
	   *  <count><type1><type2>...<typen><obj1><obj2>...<objn>
	   * }
	   */
	@Override
	public void write(DataOutput out) throws IOException
	{
		WritableUtils.writeVInt(out, values.length);
		for (int i = 0; i < values.length; ++i)
		{
			Text.writeString(out, values[i].getClass().getName());
		}
		for (int i = 0; i < values.length; ++i)
		{
			values[i].write(out);
		}

		// ByteArrayOutputStream bo;
		// ObjectOutputStream oo;
		//
		// out.writeInt(size);
		// for (int i = 0; i < size; i++)
		// {
		// bo = new ByteArrayOutputStream();
		// oo = new ObjectOutputStream(bo);
		// oo.writeObject(classes[i]);
		//
		// bytes = bo.toByteArray();
		//
		// out.writeInt(bytes.length);
		// out.write(bytes);
		// values[i].write(out);
		//
		// oo.close();
		// bo.close();
		//
		// }
	}

	/**
	 * {@inheritDoc}
	 */
	@SuppressWarnings("unchecked")
	@Override
	public void readFields(DataInput in) throws IOException
	{
		size = WritableUtils.readVInt(in);
		values = new Writable[size];
		Class<? extends Writable>[] cls = new Class[size];
		try
		{
			for (int i = 0; i < size; ++i)
			{
				cls[i] = Class.forName(Text.readString(in)).asSubclass(Writable.class);
			}
			for (int i = 0; i < size; ++i)
			{
				values[i] = cls[i].newInstance();
				values[i].readFields(in);
			}
		}
		catch (ClassNotFoundException e)
		{
			throw (IOException) new IOException("Failed tuple init").initCause(e);
		}
		catch (IllegalAccessException e)
		{
			throw (IOException) new IOException("Failed tuple init").initCause(e);
		}
		catch (InstantiationException e)
		{
			throw (IOException) new IOException("Failed tuple init").initCause(e);
		}
		// ByteArrayInputStream bo;
		// ObjectInputStream oo;
		//
		// size = in.readInt();
		// values = new Writable[size];
		// for (int i = 0; i < size; i++)
		// {
		// int length = in.readInt();
		// byte[] bytes = new byte[length];
		// in.readFully(bytes, 0, length);
		// bo = new ByteArrayInputStream(bytes);
		// oo = new ObjectInputStream(bo);
		// Class c;
		// try
		// {
		// c = (Class) oo.readObject();
		// values[i] = (Writable) c.getConstructors()[0].newInstance();
		// values[i].readFields(in);
		//
		// oo.close();
		// bo.close();
		// }
		// catch (Exception e)
		// {
		// e.printStackTrace();
		// }
		// }
	}

	private byte[] bytes;
	private int length;
	@Override
	public int getLength()
	{
		return length;
	}

	@Override
	public byte[] getBytes()
	{
		ByteArrayOutputStream bo;
		ObjectOutputStream oo;
		try
		{
			bo = new ByteArrayOutputStream();
			oo = new ObjectOutputStream(bo);
			oo.writeObject(values);

			bytes = bo.toByteArray();
			length = bytes.length;

			oo.close();
			bo.close();

			return bytes;
		}
		catch (IOException e)
		{
			e.printStackTrace();
		}
		return null;
	}

	/** A WritableComparator optimized for Text keys. */
	public static class Comparator extends WritableComparator
	{
		public Comparator()
		{
			super(Tuple.class);
		}

		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2)
		{
			int n1 = WritableUtils.decodeVIntSize(b1[s1]);
			int n2 = WritableUtils.decodeVIntSize(b2[s2]);
			return compareBytes(b1, s1 + n1, l1 - n1, b2, s2 + n2, l2 - n2);
		}
	}

	static
	{
		WritableComparator.define(Tuple.class, new Comparator());
	}
	
	/**
	  * The number of children in this Tuple.
	  */
	public int size()
	{
		return size;
	}
}