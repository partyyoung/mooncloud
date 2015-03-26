package test.local.conditionalentropy1;

import java.io.IOException;

import net.mooncloud.io.Tuple;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

//public class MrMapper extends TableMapper<LongWritable, LongWritable> {
//
//	@Override
//	public void map(InputSplitFile recordNum, Record record, Context context) throws IOException, InterruptedException {
//		context.write((LongWritable) record.get("shop_id"), (LongWritable) record.get("ipv"));
//	}
//}

public class MrMapper extends Mapper<LongWritable, Text, Tuple, Text>
{
	private Tuple key = new Tuple(2);

	private Configuration conf;

	@Override
	public void setup(Context context) throws IOException, InterruptedException
	{
		conf = context.getConfiguration();
		recordNums = conf.getLong("recordNum", 0);
	}

	private long num = 0, recordNums = 0;

	@Override
	public void map(LongWritable recordNum, Text record, Context context) throws IOException, InterruptedException
	{
		long A = num, B = 1;
		for (long i = 0; i < recordNums; i++)
		{
			B = i;

			if (A == B)
				continue;

			if (A < B)
				key.set(0, new Text(A + "-" + B));
			else key.set(0, new Text(B + "-" + A));
			key.set(1, new LongWritable(A));
			context.write(key, record);
		}
		num++;
	}
}
