package test.local.conditionalentropy;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;

import net.mooncloud.io.Tuple;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

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

public class MrReducer extends Reducer<Tuple, LongWritable, Text, DoubleWritable>
{

	private Text preKey0 = null;
	private HashMap<String, Long> Acounts = new HashMap<String, Long>();
	private HashMap<String, HashMap<String, Long>> ABcounts = new HashMap<String, HashMap<String, Long>>();

	@Override
	public void reduce(Tuple key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException
	{
		Text key0 = (Text) key.get(0);
		if (preKey0 == null)
		{
			//
			preKey0 = key0;
			Acounts.clear();
			ABcounts.clear();
			Acounts.put("TOTAL", 0L);
		}
		else if (!preKey0.equals(key0))
		{
			try
			{
				// 计算条件熵
				double ABentropy = 0.0;
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

				context.write(preKey0, new DoubleWritable(ABentropy));

			}
			catch (Exception e)
			{
				e.printStackTrace();
			}
			//
			preKey0 = key0;
			Acounts.clear();
			ABcounts.clear();
			Acounts.put("TOTAL", 0L);
		}

		String A = key.get(1).toString();
		String B = key.get(2).toString();
		for (LongWritable value : values)
		{
			long ABcount = value.get();
			Acounts.put("TOTAL", Acounts.get("TOTAL") + ABcount);
			if (Acounts.containsKey(A))
			{
				Acounts.put(A, Acounts.get(A) + ABcount);

				HashMap<String, Long> Bcounts = ABcounts.get(A);
				if (Bcounts.containsKey(B))
				{
					Bcounts.put(B, Bcounts.get(B) + ABcount);
				}
				else
				{
					Bcounts.put(B, ABcount);
				}
			}
			else
			{
				Acounts.put(A, ABcount);

				HashMap<String, Long> Bcounts = new HashMap<String, Long>();
				Bcounts.put(B, ABcount);
				ABcounts.put(A, Bcounts);
			}
		}
	}

	@Override
	public void cleanup(Context context) throws IOException, InterruptedException
	{
		// 计算条件熵
		try
		{
			double ABentropy = 0.0;
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

					Bentropy += -(1.0 * Bcount / Acount) * Math.log10(1.0 * Bcount / Acount);
				}

				ABentropy += Bentropy / TOTAL * Acount;
			}

			context.write(preKey0, new DoubleWritable(ABentropy));

			//
			preKey0 = null;
			Acounts.clear();
			ABcounts.clear();
			Acounts.put("TOTAL", 0L);
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}

}
