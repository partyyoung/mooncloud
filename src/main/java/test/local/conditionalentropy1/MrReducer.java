package test.local.conditionalentropy1;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.Entry;

import net.mooncloud.io.Tuple;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;

//public class MrReducer extends TableReducer<LongWritable, LongWritable> {
//
//	private LongWritable sum = new LongWritable();
//	private Record res = null;
//
//	@Override
//	public void setup(Context context) throws IOException, InterruptedException {
//		res = new Record(context.getConfiguration().get("mapred.output.schema"));
//	}
//
//	@Override
//	public void reduce(LongWritable key, Iterable<LongWritable> values, Context context) throws IOException,
//			InterruptedException {
//		long val = 0;
//		for (LongWritable value : values) {
//			val += value.get();
//		}
//		sum.set(val);
//
//		res.set("shop_id", key);
//		res.set("pv", sum);
//		context.write(res, NullWritable.get());
//	}
//
//}

public class MrReducer extends Reducer<Tuple, Text, Text, DoubleWritable>
{
	private Configuration conf;

	private Text preKey0 = null;
	private HashMap<String, Long> Acounts = new HashMap<String, Long>();
	private HashMap<String, HashMap<String, Long>> ABcounts = new HashMap<String, HashMap<String, Long>>();

	private ArrayList<Text> AB = new ArrayList<Text>(2);

	@Override
	public void setup(Context context) throws IOException, InterruptedException
	{
		conf = context.getConfiguration();
	}

	@Override
	public void reduce(Tuple key, Iterable<Text> values, Context context) throws IOException, InterruptedException
	{
		Text key0 = (Text) key.get(0);
		if (preKey0 == null)
		{
			//
			preKey0 = key0;
			Acounts.clear();
			ABcounts.clear();
			AB.clear();
			Acounts.put("TOTAL", 0L);
		}
		else if (!preKey0.equals(key0))
		{
			try
			{
				// 计算条件熵
				String[] Atext = AB.get(0).toString().split(conf.get("sep", ","), -1);
				String[] Btext = AB.get(1).toString().split(conf.get("sep", ","), -1);

				double ABentropy = conditionalentropy(Atext, Btext);
				context.write(preKey0, new DoubleWritable(ABentropy));
				Acounts.clear();
				ABcounts.clear();
				AB.clear();
				Acounts.put("TOTAL", 0L);

				String keys[] = preKey0.toString().split("-");
				double ABentropy1 = conditionalentropy(Btext, Atext);
				context.write(new Text(keys[1] + "-" + keys[0]), new DoubleWritable(ABentropy1));
			}
			catch (Exception e)
			{
				e.printStackTrace();
			}
			//
			preKey0 = key0;
			Acounts.clear();
			ABcounts.clear();
			AB.clear();
			Acounts.put("TOTAL", 0L);
		}

		String A = key.get(1).toString();
		Text value = values.iterator().next();
		AB.add(new Text(value));
	}

	@Override
	public void cleanup(Context context) throws IOException, InterruptedException
	{
		// 计算条件熵
		try
		{
			// 计算条件熵
			String[] Atext = AB.get(0).toString().split(conf.get("sep", ","), -1);
			String[] Btext = AB.get(1).toString().split(conf.get("sep", ","), -1);

			double ABentropy = conditionalentropy(Atext, Btext);
			context.write(preKey0, new DoubleWritable(ABentropy));
			Acounts.clear();
			ABcounts.clear();
			AB.clear();
			Acounts.put("TOTAL", 0L);

			String keys[] = preKey0.toString().split("-");
			double ABentropy1 = conditionalentropy(Btext, Atext);
			context.write(new Text(keys[1] + "-" + keys[0]), new DoubleWritable(ABentropy1));

			//
			preKey0 = null;
			Acounts.clear();
			ABcounts.clear();
			AB.clear();
			Acounts.put("TOTAL", 0L);
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}

	private double conditionalentropy(String[] AA, String[] BB)
	{
		double ABentropy = 0.0;

		Acounts.put("TOTAL", 0L);
		for (int i = 0; i < AA.length; i++)
		{
			String A = AA[i], B = BB[i];
			Acounts.put("TOTAL", Acounts.get("TOTAL") + 1);
			if (Acounts.containsKey(A))
			{
				Acounts.put(A, Acounts.get(A) + 1);

				HashMap<String, Long> Bcounts = ABcounts.get(A);
				if (Bcounts.containsKey(B))
				{
					Bcounts.put(B, Bcounts.get(B) + 1);
				}
				else
				{
					Bcounts.put(B, 1L);
				}
			}
			else
			{
				Acounts.put(A, 1L);

				HashMap<String, Long> Bcounts = new HashMap<String, Long>();
				Bcounts.put(B, 1L);
				ABcounts.put(A, Bcounts);
			}
		}

		long TOTAL = Acounts.remove("TOTAL");
		for (Entry<String, Long> entryA : Acounts.entrySet())
		{
			String A = entryA.getKey();
			long Acount = entryA.getValue();

			double Bentropy = 0.0;
			HashMap<String, Long> Bcounts = ABcounts.get(A);
			for (Entry<String, Long> entryB : Bcounts.entrySet())
			{
				String B = entryB.getKey();
				long Bcount = entryB.getValue();

				Bentropy += -(1.0 * Bcount / Acount) * Math.log10(1.0 * Bcount / Acount) / Math.log10(2.0);
			}

			ABentropy += Bentropy / TOTAL * Acount;
		}
		return ABentropy;
	}

}
