package mobile.recommend;

import java.io.IOException;

import net.mooncloud.Record;
import net.mooncloud.io.InputSplitFile;
import net.mooncloud.mapreduce.TableMapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class MrMapper extends TableMapper<Text, LongWritable>
{

	@Override
	public void map(InputSplitFile recordNum, Record record, Context context) throws IOException, InterruptedException
	{
		long behavior_type = ((LongWritable) record.get("behavior_type")).get();
		if (behavior_type == 4)
			context.write(new Text(((Text) record.get("user_id")) + "\001" + ((Text) record.get("item_id"))), new LongWritable(1L));
	}
}
