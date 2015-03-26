package test.local.kmeans;

import java.util.Map;

import net.mooncloud.mapreduce.lib.jobcontrol.ControlledJob;
import net.mooncloud.mapreduce.lib.jobcontrol.JobControl;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class Run
{
	private static final Log LOG = LogFactory.getLog(Run.class);

	public static void main(String[] args) throws Exception
	{
		long start = System.nanoTime();

		MrRun mrRun = new MrRun();
		Map mr_conf = null;

		String kmeans_MBP_MR = "test/local/kmeans/mbp_mr.yaml";
		mr_conf = MrRun.loadMrConf(kmeans_MBP_MR);
		ControlledJob kmeans_preprocess_ctrlJob = mrRun.makeControlledJob(mr_conf, args);
		if (kmeans_preprocess_ctrlJob == null)
		{
			LOG.error("配置错误，无法构建 map/reduce 任务！");
			return;
		}

		// XXX JobControl
		JobControl jobControl = new JobControl("kmeans");
		jobControl.addJob(kmeans_preprocess_ctrlJob);

		// XXX start
		Thread theController = new Thread(jobControl);
		theController.start();
		while (!jobControl.allFinished())
		{
			Thread.sleep(5000);
		}
		LOG.info("map/reduce job all finished！");
		jobControl.stop();

		long end = System.nanoTime();
		LOG.info("运行时间: " + (end - start) / 1e9 + " seconds");
	}
}
