package net.mooncloud.quartz;

import java.io.IOException;
import java.lang.Thread.State;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.util.StringUtils;

/**
 * @see net.mooncloud.mapreduce.lib.jobcontrol.ControlledJob
 * @author jiandang
 * 
 */
@Deprecated
public class ControlledJob
{
	private static final Log LOG = LogFactory.getLog(ControlledJob.class);

	public static enum JobState
	{
		SUCCESS, WAITING, RUNNING, READY, FAILED, DEPENDENT_FAILED
	};

	private JobState jobState = JobState.READY;
	private String controlID; // assigned and used by JobControl class
	private String jobName;
	// private ControlledJob job; // mapreduce job to be executed.
	private Thread job; // mapreduce job to be executed.
	private String message;
	private List<ControlledJob> dependingJobs;

	/**
	 * Construct a job.
	 * 
	 * @param job
	 *            a mapreduce job to be executed.
	 * @param dependingJobs
	 *            an array of jobs the current job depends on
	 */
	public ControlledJob(Runnable job, List<ControlledJob> dependingJobs) throws IOException
	{
		this.job = new Thread(job);
		this.dependingJobs = dependingJobs;
		this.jobState = JobState.WAITING;
		this.controlID = "unassigned";
		this.jobName = job.getClass().toString();
		this.message = "just initialized";
	}

	@Override
	public String toString()
	{
		StringBuffer sb = new StringBuffer();
		sb.append("job id:\t").append(this.controlID).append("\n");
		sb.append("job name:\t").append(this.getJobName()).append("\n");
		sb.append("job jobState:\t").append(this.jobState).append("\n");
		sb.append("job id:\t").append(this.getJobID()).append("\n");
		sb.append("job message:\t").append(this.message).append("\n");

		if (this.dependingJobs == null || this.dependingJobs.size() == 0)
		{
			sb.append("job has no depending job:\t").append("\n");
		}
		else
		{
			sb.append("job has ").append(this.dependingJobs.size()).append(" dependeng jobs:\n");
			for (int i = 0; i < this.dependingJobs.size(); i++)
			{
				sb.append("\t depending job ").append(i).append(":\t");
				sb.append((this.dependingJobs.get(i)).getJobName()).append("\n");
			}
		}
		return sb.toString();
	}

	/**
	 * @return the job name of this job
	 */
	public String getJobName()
	{
		return this.jobName;
	}

	/**
	 * Set the job name for this job.
	 * 
	 * @param jobName
	 *            the job name
	 */
	public void setJobName(String jobName)
	{
		this.jobName = jobName;
	}

	/**
	 * @return the job ID of this job assigned by JobControl
	 */
	public String getJobID()
	{
		return this.controlID;
	}

	/**
	 * Set the job ID for this job.
	 * 
	 * @param id
	 *            the job ID
	 */
	public void setJobID(String id)
	{
		this.controlID = id;
	}

	/**
	 * @return the mapred ID of this job as assigned by the mapred framework.
	 */
	// public JobID getMapredJobID()
	// {
	// return this.getJobID();
	// }

	/**
	 * @return the mapreduce job
	 */
	public synchronized ControlledJob getJob()
	{
		return this;
	}

	/**
	 * Set the mapreduce job
	 * 
	 * @param job
	 *            the mapreduce job for this job.
	 */
	// public synchronized void setJob(ControlledJob job)
	// {
	// this.job = job;
	// }

	/**
	 * @return the jobState of this job
	 */
	public synchronized JobState getJobState()
	{
		return this.jobState;
	}

	/**
	 * Set the jobState for this job.
	 * 
	 * @param jobState
	 *            the new jobState for this job.
	 */
	protected synchronized void setJobState(JobState jobState)
	{
		this.jobState = jobState;
	}

	/**
	 * @return the message of this job
	 */
	public synchronized String getMessage()
	{
		return this.message;
	}

	/**
	 * Set the message for this job.
	 * 
	 * @param message
	 *            the message for this job.
	 */
	public synchronized void setMessage(String message)
	{
		this.message = message;
	}

	/**
	 * @return the depending jobs of this job
	 */
	public List<ControlledJob> getDependentJobs()
	{
		return this.dependingJobs;
	}

	/**
	 * Add a job to this jobs' dependency list. Dependent jobs can only be added while a ControlledJob is waiting to run, not during or afterwards.
	 * 
	 * @param dependingJob
	 *            ControlledJob that this ControlledJob depends on.
	 * @return <tt>true</tt> if the ControlledJob was added.
	 */
	public synchronized boolean addDependingJob(ControlledJob dependingJob)
	{
		if (this.jobState == JobState.WAITING)
		{ // only allowed to add jobs when waiting
			if (this.dependingJobs == null)
			{
				this.dependingJobs = new ArrayList<ControlledJob>();
			}
			return this.dependingJobs.add(dependingJob);
		}
		else
		{
			return false;
		}
	}

	/**
	 * @return true if this job is in a complete jobState
	 */
	public synchronized boolean isCompleted()
	{
		return this.jobState == JobState.FAILED || this.jobState == JobState.DEPENDENT_FAILED || this.jobState == JobState.SUCCESS;
	}

	/**
	 * @return true if this job is in READY jobState
	 */
	public synchronized boolean isReady()
	{
		return this.jobState == JobState.READY;
	}

	// public void killJob() throws IOException, InterruptedException
	// {
	// this.killJob();
	// }

	public synchronized void failJob(String message) throws IOException, InterruptedException
	{
		try
		{
			if (this != null && this.jobState == JobState.RUNNING)
			{
				// this.killJob();
			}
		}
		finally
		{
			this.jobState = JobState.FAILED;
			this.message = message;
		}
	}

	/**
	 * Check the jobState of this running job. The jobState may remain the same, become SUCCESS or FAILED.
	 */
	private void checkRunningState() throws IOException, InterruptedException
	{
		try
		{
			if (this.job.getState() == State.TERMINATED)
			{
				this.jobState = JobState.SUCCESS;
			}
			// else
			// {
			// this.jobState = JobState.FAILED;
			// this.message = "ControlledJob failed!";
			// }
		}
		catch (Exception ioe)
		{
			this.jobState = JobState.FAILED;
			this.message = StringUtils.stringifyException(ioe);
			// try
			// {
			// if (job != null)
			// {
			// job.killJob();
			// }
			// }
			// catch (IOException e)
			// {
			// }
		}
	}

	/**
	 * Check and update the jobState of this job. The jobState changes depending on its current jobState and the states of the depending jobs.
	 */
	synchronized JobState checkState() throws IOException, InterruptedException
	{
		if (this.jobState == JobState.RUNNING)
		{
			checkRunningState();
		}
		if (this.jobState != JobState.WAITING)
		{
			return this.jobState;
		}
		if (this.dependingJobs == null || this.dependingJobs.size() == 0)
		{
			this.jobState = JobState.READY;
			return this.jobState;
		}
		ControlledJob pred = null;
		int n = this.dependingJobs.size();
		for (int i = 0; i < n; i++)
		{
			pred = this.dependingJobs.get(i);
			JobState s = pred.checkState();
			if (s == JobState.WAITING || s == JobState.READY || s == JobState.RUNNING)
			{
				break; // a pred is still not completed, continue in WAITING
				// jobState
			}
			if (s == JobState.FAILED || s == JobState.DEPENDENT_FAILED)
			{
				this.jobState = JobState.DEPENDENT_FAILED;
				this.message = "depending job " + i + " with jobID " + pred.getJobID() + " failed. " + pred.getMessage();
				break;
			}
			// pred must be in success jobState
			if (i == n - 1)
			{
				this.jobState = JobState.READY;
			}
		}

		return this.jobState;
	}

	/**
	 * Submit this job to mapred. The jobState becomes RUNNING if submission is successful, FAILED otherwise.
	 */
	protected synchronized void submit()
	{
		try
		{
			this.job.start();
			this.jobState = JobState.RUNNING;
		}
		catch (Exception ioe)
		{
			LOG.info(getJobName() + " got an error while submitting ", ioe);
			this.jobState = JobState.FAILED;
			this.message = StringUtils.stringifyException(ioe);
		}
	}
	//
	// public boolean waitForCompletion(boolean verbose) throws IOException,
	// InterruptedException,
	// ClassNotFoundException, SQLException
	// {
	// try
	// {
	// Configuration conf = job.getConfiguration();
	// if (conf.getBoolean(CREATE_DIR, false))
	// {
	// FileSystem fs = FileSystem.get(conf);
	// Path inputPaths[] = FileInputFormat.getInputPaths(job);
	// for (int i = 0; i < inputPaths.length; i++)
	// {
	// if (!fs.exists(inputPaths[i]))
	// {
	// try
	// {
	// fs.mkdirs(inputPaths[i]);
	// }
	// catch (IOException e)
	// {
	//
	// }
	// }
	// }
	// }
	// boolean isSuccessful = job.waitForCompletion(verbose);
	// return isSuccessful;
	// }
	// catch (Exception ioe)
	// {
	// LOG.info(getJobName() + " got an error while submitting ", ioe);
	// this.jobState = JobState.FAILED;
	// this.message = StringUtils.stringifyException(ioe);
	// return false;
	// }
	// }

}
