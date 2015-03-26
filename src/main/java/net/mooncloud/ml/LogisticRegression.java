package net.mooncloud.ml;

import java.util.Random;

public class LogisticRegression
{
	private double Goal = 1e-3; // 目标误差
	private long Epochs = 300000; // 训练次数
	private double alpha = 0.9; // 0.9~1 缓冲参数
	private double eta = 0.1; // 0.1~3 下降速度

	private double[] W, theta_W;
	private int N; // 变量个数

	public LogisticRegression(int N)
	{
		this.N = N;
		W = new double[N + 1];
		theta_W = new double[N + 1];
	}

	public double Identify(final double input[], final double[] W)
	{
		this.W = W;
		return Identify(input);
	}

	public double Identify(final double input[])
	{
		double z = W[this.N];
		for (int i = 0; i < this.N; i++)
		{
			z += W[i] * input[i];
		}

		return 1 / (1 + Math.pow(Math.E, -z));
	}

	public double[] TrainingSample(double inputs[][], double outputs[])
	{
		if (inputs == null || inputs.length == 0 || inputs[0].length != this.N || outputs == null || inputs.length != outputs.length)
		{
			return null;
		}

		// 初始化权值
		Random r = new Random();
		for (int i = 0; i < this.N + 1; i++)
		{
			W[i] = r.nextDouble() * 0.6 - 0.3;
		}

		// 开始训练
		double Ek = 0, E;
		int count = 0;// 训练次数
		do
		{
			double[] YY = new double[outputs.length];
			for (int i = 0; i < inputs.length; i++)
			{
				double Yi = outputs[i];
				double Y = Identify(inputs[i]);
				YY[i] = Y - Yi;
			}
			// 计算误差E
			E = 0.0;
			for (int i = 0; i < YY.length; i++)
			{
				E += Math.pow(YY[i], 2) / 2.0;
			}
			if (E < Goal)
			{
				// 达到预定误差
				// printf("\n%d:%e\n", count, E);
				System.out.println("训练次数: " + count + "\t误差值: " + E + "\n--------------------------------------------");
				return W;
			}

			// 梯度下降 计算δ
			double[] theta = new double[this.N + 1];
			for (int i = 0; i < inputs.length; i++)
			{
				for (int j = 0; j < this.N; j++)
				{
					theta[j] += YY[i] * inputs[i][j];
				}
				theta[this.N] += YY[i];
			}// 一次训练结束

			// 计算并保存各权值修正值，并修正权值
			for (int i = 0; i < this.N + 1; i++)
			{
				theta_W[i] = alpha * theta_W[i] + eta * theta[i];
				W[i] += theta_W[i];
			}

			// 自适应学习速度， eta-η
			if (Ek > 0)
			{
				if (E < Ek)
				{
					eta = 1.0285 * eta;
				}
				else if (E > 1.04 * Ek)
				{
					eta = 0.7 * eta;
				}
			}
			Ek = E;
		}
		while (++count < Epochs);// 训练结束

		System.out.println("训练次数: " + count + "\t误差值: " + E + "\n--------------------------------------------");
		return W;
	}

	public static void main(String[] args)
	{
		int l = 2;
		double inputs[][] = new double[][]
		{
		{ 1 },
		{ 0 } }, outputs[] = new double[]
		{ 1, 0 };

		LogisticRegression lr = new LogisticRegression(1);
		double W[] = lr.TrainingSample(inputs, outputs);

		for (int i = 0; i < W.length; i++)
		{
			System.out.print(W[i] + "\t");
		}
		System.out.println("--------------------------------------------");

		double input[] = inputs[0];
		double output = lr.Identify(input);
		for (int i = 0; i < input.length; i++)
		{
			System.out.print(input[i] + "\t");
		}
		System.out.println();
		System.out.print(output + "\t");
	}
}
