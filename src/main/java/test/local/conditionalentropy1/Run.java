package test.local.conditionalentropy1;

import java.net.URI;

import net.mooncloud.io.Tuple;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Run implements Tool
{

	Configuration conf = new Configuration();

	@Override
	public void setConf(Configuration conf)
	{
		// TODO Auto-generated method stub
		this.conf = conf;
	}

	@Override
	public Configuration getConf()
	{
		// TODO Auto-generated method stub
		return this.conf;
	}

	@Override
	public int run(String[] args) throws Exception
	{
		// 设置 MR 本地运行环境
		if (this.conf.getBoolean("local", true))
		{
			conf.set("mapred.job.tracker", "local");
			conf.set("fs.default.name", "file:///home/yangjd/Documents/workspace/");
			// System.setProperty("hadoop.home.dir",
			// "file:///D:/My Documents/Downloads/hadoop-2.3.0");
		}
		conf.setBoolean("mapreduce.input.fileinputformat.input.dir.recursive", true);

		Job job1 = new Job(conf);
		job1.setJobName("evaluate feature subset");
		job1.setJarByClass(Run.class);

		String input = "file:///home/yangjd/Documents/workspace/mooncloud/warehouse/mr_dw/weather_transpose/";
		String output = "file:///home/yangjd/Documents/workspace/mooncloud/warehouse/pri_result/conditionalentropy1";

		input = this.conf.get("input", input);
		output = this.conf.get("output", output);

		FileInputFormat.addInputPath(job1, new Path(input));
		FileOutputFormat.setOutputPath(job1, new Path(output));

		job1.setMapperClass(MrMapper.class);
		// job1.setCombinerClass(MrCombiner.class);
		job1.setReducerClass(MrReducer.class);

		job1.setPartitionerClass(MrPartitioner.class);
		// job1.setGroupingComparatorClass(FeatureGroupComparator.class);

		job1.setNumReduceTasks(this.conf.getInt("reduceTasks", 1)); // (m + 1) /
																	// 2

		job1.setOutputKeyClass(Tuple.class);
		job1.setOutputValueClass(Text.class);

		FileSystem fstm = FileSystem.get(URI.create(output), conf);
		Path outDir = new Path(output);
		fstm.delete(outDir, true);

		job1.waitForCompletion(true);

		System.out.println(job1.getConfiguration().get("CCCCC"));

		return 0;
	}

	public static void main(String[] args) throws Exception
	{
		long start = System.nanoTime();
		int exitCode = ToolRunner.run(new Run(), args);
		long end = System.nanoTime();
		System.out.println("time = " + (end - start) / 1e9 + " seconds");
		System.exit(exitCode);
	}
}
