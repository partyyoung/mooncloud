package test.local.kmeans;

import java.io.IOException;

import net.mooncloud.Record;
import net.mooncloud.io.InputSplitFile;
import net.mooncloud.mapreduce.TableMapper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;

public class MrMapper extends TableMapper<LongWritable, LongWritable>
{
	private Configuration conf;

	private double[][] kmeans;

	@Override
	public void setup(Context context) throws IOException, InterruptedException
	{
		conf = context.getConfiguration();
		kmeans = new double[conf.getInt("K", 2)][conf.getInt("attr", 1)];
		Path kmeansFile = new Path(conf.get("kmeans"));
		FSDataInputStream fsdis = kmeansFile.getFileSystem(conf).open(kmeansFile);
		String kmean;
		while ((kmean = fsdis.readLine()) != null)
		{
			String kmeanss[] = kmean.split(",");
			int k = Integer.parseInt(kmeanss[0]);
			for (int i = 1; i < kmeanss.length; i++)
			{
				kmeans[k][i] = Double.parseDouble(kmeanss[i]);
			}
		}
		fsdis.close();
	}

	@Override
	public void map(InputSplitFile recordNum, Record record, Context context) throws IOException, InterruptedException
	{
		context.write((LongWritable) record.get("shop_id"), (LongWritable) record.get("ipv"));
	}
}
