package test.local;

import java.io.IOException;

import net.mooncloud.Record;
import net.mooncloud.mapreduce.TableReducer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;

public class MrReducer extends TableReducer<LongWritable, LongWritable> {

	private LongWritable sum = new LongWritable();
	private Record res = null;

	@Override
	public void setup(Context context) throws IOException, InterruptedException {
		res = new Record(context.getConfiguration().get("mapred.output.schema"));
	}

	@Override
	public void reduce(LongWritable key, Iterable<LongWritable> values, Context context) throws IOException,
			InterruptedException {
		long val = 0;
		for (LongWritable value : values) {
			val += value.get();
		}
		sum.set(val);

		res.set("shop_id", key);
		res.set("pv", sum);
		context.write(res, NullWritable.get());
	}

}
