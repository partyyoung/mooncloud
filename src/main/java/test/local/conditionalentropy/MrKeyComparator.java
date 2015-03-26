package test.local.conditionalentropy;

import net.mooncloud.io.Tuple;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;


public class MrKeyComparator extends WritableComparator
{
	/**
	 * 
	 */
	public MrKeyComparator()
	{
		super(Tuple.class, true);
	}
	@Override
	public int compare(WritableComparable o1, WritableComparable o2)
	{
		Tuple t1 = (Tuple) o1;
		Tuple t2 = (Tuple) o2;
		Text key1 = ((Text) t1.get(0));
		Text key2 = ((Text) t2.get(0));
		int flag1 = ((IntWritable) t1.get(1)).get();
		int flag2 = ((IntWritable) t2.get(1)).get();
		Text tag1 = ((Text) t1.get(2));
		Text tag2 = ((Text) t2.get(2));
		return (key1.equals(key2)
				? (flag1 == flag2 ? tag2.compareTo(tag1) : flag1 > flag2 ? -1 : 1)
				: (key2.compareTo(key1))); // --DESC
	}
}