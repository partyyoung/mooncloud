package mobile.recommend;

import java.io.IOException;

import net.mooncloud.Record;
import net.mooncloud.mapreduce.TableReducer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

public class MrReducer extends TableReducer<Text, LongWritable>
{

	private LongWritable sum = new LongWritable();
	private Record res = null;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException
	{
		res = new Record(context.getConfiguration().get("mapred.output.schema"));
	}

	@Override
	public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException
	{
		String[] user_item = key.toString().split("\001");

//		long val = 0;
//		for (LongWritable value : values)
//		{
//			val += value.get();
//		}
//		sum.set(val);

		res.set("user_id", new Text(user_item[0]));
		res.set("item_id", new Text(user_item[1]));
		context.write(res, NullWritable.get());
	}

}
