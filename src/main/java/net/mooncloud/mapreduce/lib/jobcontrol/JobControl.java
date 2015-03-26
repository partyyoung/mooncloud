package net.mooncloud.mapreduce.lib.jobcontrol;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import net.mooncloud.mapreduce.lib.jobcontrol.ControlledJob.State;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.util.StringUtils;

@InterfaceAudience.Public
@InterfaceStability.Evolving
public class JobControl implements Runnable
{

	private static final Log LOG = LogFactory.getLog(JobControl.class);

	// The thread can be in one of the following state
	public static enum ThreadState
	{
		RUNNING, SUSPENDED, STOPPED, STOPPING, READY
	};

	private ThreadState runnerState; // the thread state

	private LinkedList<ControlledJob> jobsInProgress = new LinkedList<ControlledJob>();
	private LinkedList<ControlledJob> successfulJobs = new LinkedList<ControlledJob>();
	private LinkedList<ControlledJob> failedJobs = new LinkedList<ControlledJob>();

	private long nextJobID;
	private String groupName;

	/**
	 * Construct a job control for a group of jobs.
	 * 
	 * @param groupName
	 *            a name identifying this group
	 */
	public JobControl(String groupName)
	{
		this.nextJobID = -1;
		this.groupName = groupName;
		this.runnerState = ThreadState.READY;
	}

	private static List<ControlledJob> toList(LinkedList<ControlledJob> jobs)
	{
		ArrayList<ControlledJob> retv = new ArrayList<ControlledJob>();
		synchronized (jobs)
		{
			for (ControlledJob job : jobs)
			{
				retv.add(job);
			}
		}
		return retv;
	}

	synchronized private List<ControlledJob> getJobsIn(State state)
	{
		LinkedList<ControlledJob> l = new LinkedList<ControlledJob>();
		for (ControlledJob j : jobsInProgress)
		{
			if (j.getJobState() == state)
			{
				l.add(j);
			}
		}
		return l;
	}

	/**
	 * @return the jobs in the waiting state
	 */
	public List<ControlledJob> getWaitingJobList()
	{
		return getJobsIn(State.WAITING);
	}

	/**
	 * @return the jobs in the running state
	 */
	public List<ControlledJob> getRunningJobList()
	{
		return getJobsIn(State.RUNNING);
	}

	/**
	 * @return the jobs in the ready state
	 */
	public List<ControlledJob> getReadyJobsList()
	{
		return getJobsIn(State.READY);
	}

	/**
	 * @return the jobs in the success state
	 */
	public List<ControlledJob> getSuccessfulJobList()
	{
		return toList(this.successfulJobs);
	}

	public List<ControlledJob> getFailedJobList()
	{
		return toList(this.failedJobs);
	}

	private String getNextJobID()
	{
		nextJobID += 1;
		return this.groupName + this.nextJobID;
	}

	/**
	 * Add a new job.
	 * 
	 * @param aJob
	 *            the new job
	 */
	synchronized public String addJob(ControlledJob aJob)
	{
		String id = this.getNextJobID();
		aJob.setJobID(id);
		aJob.setJobState(State.WAITING);
		jobsInProgress.add(aJob);
		return id;
	}

	/**
	 * Add a collection of jobs
	 * 
	 * @param jobs
	 */
	public void addJobCollection(Collection<ControlledJob> jobs)
	{
		for (ControlledJob job : jobs)
		{
			addJob(job);
		}
	}

	/**
	 * @return the thread state
	 */
	public ThreadState getThreadState()
	{
		return this.runnerState;
	}

	/**
	 * set the thread state to STOPPING so that the thread will stop when it wakes up.
	 */
	public void stop()
	{
		this.runnerState = ThreadState.STOPPING;
	}

	/**
	 * suspend the running thread
	 */
	public void suspend()
	{
		if (this.runnerState == ThreadState.RUNNING)
		{
			this.runnerState = ThreadState.SUSPENDED;
		}
	}

	/**
	 * resume the suspended thread
	 */
	public void resume()
	{
		if (this.runnerState == ThreadState.SUSPENDED)
		{
			this.runnerState = ThreadState.RUNNING;
		}
	}

	synchronized public boolean allFinished()
	{
		return jobsInProgress.isEmpty();
	}

	/**
	 * The main loop for the thread. The loop does the following: Check the states of the running jobs Update the states of waiting jobs Submit the jobs in ready state
	 */
	public void run()
	{
		try
		{
			this.runnerState = ThreadState.RUNNING;
			while (true)
			{
				while (this.runnerState == ThreadState.SUSPENDED)
				{
					try
					{
						Thread.sleep(5000);
					}
					catch (Exception e)
					{
						// TODO the thread was interrupted, do something!!!
					}
				}

				synchronized (this)
				{
					Iterator<ControlledJob> it = jobsInProgress.iterator();
					while (it.hasNext())
					{
						ControlledJob j = it.next();
						LOG.debug("Checking state of job " + j);
						switch (j.checkState())
						{
							case SUCCESS :
								LOG.info("Success job " + j);
								j.loadDataToDB();
								successfulJobs.add(j);
								it.remove();
								break;
							case FAILED :
							case DEPENDENT_FAILED :
								LOG.info("Failed job " + j);
								failedJobs.add(j);
								it.remove();
								break;
							case READY :
								LOG.info("Submit job " + j);
								j.submit();
								break;
							case RUNNING :
							case WAITING :
								// Do Nothing
								break;
						}
					}
				}

				if (this.runnerState != ThreadState.RUNNING
						&& this.runnerState != ThreadState.SUSPENDED)
				{
					break;
				}
				try
				{
					Thread.sleep(5000);
				}
				catch (Exception e)
				{
					// TODO the thread was interrupted, do something!!!
				}
				if (this.runnerState != ThreadState.RUNNING
						&& this.runnerState != ThreadState.SUSPENDED)
				{
					break;
				}
			}
		}
		catch (Throwable t)
		{
			LOG.error("Error while trying to run jobs.", t);
			// Mark all jobs as failed because we got something bad.
			failAllJobs(t);
		}
		this.runnerState = ThreadState.STOPPED;
	}

	synchronized private void failAllJobs(Throwable t)
	{
		String message = "Unexpected System Error Occured: " + StringUtils.stringifyException(t);
		Iterator<ControlledJob> it = jobsInProgress.iterator();
		while (it.hasNext())
		{
			ControlledJob j = it.next();
			try
			{
				j.failJob(message);
			}
			catch (IOException e)
			{
				LOG.error("Error while tyring to clean up " + j.getJobName(), e);
			}
			catch (InterruptedException e)
			{
				LOG.error("Error while tyring to clean up " + j.getJobName(), e);
			}
			finally
			{
				failedJobs.add(j);
				it.remove();
			}
		}
	}
}
