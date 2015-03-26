package net.mooncloud.quartz;

import java.io.IOException;

public class test
{
	public static void main(String[] args) throws InterruptedException, IOException
	{
		ControlledJob job1 = new ControlledJob(new Job1(), null);
		ControlledJob job2 = new ControlledJob(new Job2(), null);

		JobControl jobControl = new JobControl("TEST");// XXX start

//		job2.addDependingJob(job1);

		jobControl.addJob(job1);
		jobControl.addJob(job2);

		Thread theController = new Thread(jobControl);
		theController.start();
		while (!jobControl.allFinished())
		{
			Thread.sleep(5000);
		}
		jobControl.stop();
	}
}

class Job1 implements Runnable
{
	@Override
	public void run()
	{
		System.out.println("hello job1 running");
		try
		{
			Thread.sleep(20 * 1000 * 1);
		}
		catch (InterruptedException e)
		{
			e.printStackTrace();
		}
	}

}

class Job2 implements Runnable
{
	@Override
	public void run()
	{
		try
		{
			System.out.println("hello job2 running");
			Thread.sleep(20 * 1000 * 1);
		}
		catch (InterruptedException e)
		{
			e.printStackTrace();
		}
	}
}
