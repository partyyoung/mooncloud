package net.mooncloud.mapreduce.run;

import java.util.Map;

import net.mooncloud.mapreduce.lib.jobcontrol.ControlledJob;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * @author yangjd
 *
 */
public class RunLocal {
	private static final Log LOG = LogFactory.getLog(RunLocal.class);

	public static void main(String[] args) throws Exception {
		long start = System.nanoTime();

		MrRun mrRun = new MrRun();
		Map mr_conf = null;
		// XXX error
		mrRun.TEST_DATE_YMD = "20151231";
		String behavior_data_MBP_MR = "cdrcall/statistics/p0/local of mbp_mr.yaml";
		mr_conf = MrRun.loadMrConf(behavior_data_MBP_MR);
		ControlledJob behavior_data_ctrlJob = mrRun.makeControlledJob(mr_conf,
				args);
		if (behavior_data_ctrlJob == null) {
			LOG.error("配置错误，无法构建 map/reduce 任务！");
			return;
		}
		behavior_data_ctrlJob.waitForCompletion(true);

		long end = System.nanoTime();
		LOG.info("运行时间: " + (end - start) / 1e9 + " seconds");
	}
}
