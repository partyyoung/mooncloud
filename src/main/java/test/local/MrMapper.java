package test.local;

import java.io.IOException;

import net.mooncloud.Record;
import net.mooncloud.io.InputSplitFile;
import net.mooncloud.mapreduce.TableMapper;

import org.apache.hadoop.io.LongWritable;

public class MrMapper extends TableMapper<LongWritable, LongWritable> {

	@Override
	public void map(InputSplitFile recordNum, Record record, Context context) throws IOException, InterruptedException {
		context.write((LongWritable) record.get("shop_id"), (LongWritable) record.get("ipv"));
	}
}
