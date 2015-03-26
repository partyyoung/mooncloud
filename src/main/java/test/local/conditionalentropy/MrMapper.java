package test.local.conditionalentropy;

import java.io.IOException;

import net.mooncloud.Record;
import net.mooncloud.io.InputSplitFile;
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

public class MrMapper extends Mapper<LongWritable, Text, Tuple, LongWritable>
{
	private Tuple key = new Tuple(3);

	private Configuration conf;

	@Override
	public void setup(Context context) throws IOException, InterruptedException
	{
		conf = context.getConfiguration();
	}

	@Override
	public void map(LongWritable recordNum, Text record, Context context) throws IOException, InterruptedException
	{
		String[] lines = record.toString().split(conf.get("sep", ","), -1);
		int A = 0, B = 1;
		for (int i = 0; i < lines.length - 1; i++)
		{
			for (int j = i + 1; j < lines.length; j++)
			{
				A = i;
				B = j;

				key.set(0, new Text(A + "-" + B));
				key.set(1, new Text(lines[A] + ""));
				key.set(2, new Text("" + lines[B]));
				context.write(key, new LongWritable(1));

				key.set(0, new Text(B + "-" + A));
				key.set(1, new Text(lines[B] + ""));
				key.set(2, new Text("" + lines[A]));
				context.write(key, new LongWritable(1));
			}
		}
	}
}
