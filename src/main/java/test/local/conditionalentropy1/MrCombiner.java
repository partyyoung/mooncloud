package test.local.conditionalentropy1;

import java.io.IOException;

import net.mooncloud.io.Tuple;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class MrCombiner extends
		Reducer<Tuple, LongWritable, Tuple, LongWritable> {

	private LongWritable res = new LongWritable();

	@Override
	protected void reduce(Tuple key, Iterable<LongWritable> values,
			Context context) throws IOException, InterruptedException {
		long val = 0;

		for (LongWritable value : values) {
			val += value.get();
		}

		res.set(val);
		context.write(key, res);
	}

}
