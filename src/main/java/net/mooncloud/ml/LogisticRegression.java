package net.mooncloud.ml;

import java.util.Arrays;
import java.util.Random;

public class LogisticRegression
{
	private double Goal = 1e-3; // 目标误差
	private long Epochs = 200; // 训练次数
	private double alpha = 0.9; // 0.9~1 缓冲参数
	private double eta = 0.1; // 0.1~3 下降速度

	private double[] W, theta_W;
	private static int N; // 变量个数

	public LogisticRegression(int N)
	{
		this.N = N;
		W = new double[N + 1];
		theta_W = new double[N + 1];
	}

	public double Identify(final double input[], final double[] W)
	{
		this.W = W;
		return identify(input);
	}

	public double identify(final double input[])
	{
		double z = W[this.N];
		for (int i = 0; i < this.N; i++)
		{
			z += W[i] * input[i];
		}

		return 1 / (1 + Math.exp(-z));
	}

	public double identify(final double w[], final double input[])
	{
		double z = 0.0;
		for (int i = 0; i < this.N; i++)
		{
			z += w[i] * input[i];
		}

		return 1 / (1 + Math.exp(-z));
	}

	public static double dot(double[] a, double[] b)
	{
		double x = 0;
		for (int i = 0; i < N; i++)
		{
			x += a[i] * b[i];
		}
		return x;
	}

	public static void printWeights(double[] a)
	{
		System.out.println(Arrays.toString(a));
	}

	public double[] gradient(double weights[], double input[], double output)
	{
		double[] gradient = new double[N];
		for (int i = 0; i < N; i++)
		{
			double dot = dot(weights, input);
			gradient[i] = (1 / (1 + Math.exp(-1 * dot)) - output) * input[i];
		}
		return gradient;
	}

	public double[] vectorSum(double[] a, double[] b)
	{
		double[] result = new double[N];
		for (int j = 0; j < N; j++)
		{
			result[j] = a[j] + b[j];
		}
		return result;
	}

	private static final Random rand = new Random(42);

	public double[] training(double inputs[][], double outputs[])
	{
		double[] w = new double[this.N];
		for (int i = 0; i < N; i++)
		{
			w[i] = 2 * rand.nextDouble() - 1;
		}
		System.out.print("Initial w: ");
		printWeights(w);

		double Ek = 0, E = 0;

		for (int i = 1; i <= Epochs; i++)
		{
			double[] YY = new double[outputs.length];
			for (int ii = 0; ii < inputs.length; ii++)
			{
				double Yi = outputs[ii];
				double Y = identify(w, inputs[ii]);
				YY[ii] = Y - Yi;
			}
			// 计算误差E
			E = 0.0;
			for (int ii = 0; ii < YY.length; ii++)
			{
				E += Math.pow(YY[ii], 2) / 2.0;
			}
			if (E < Goal)
			{
				// 达到预定误差
				// printf("\n%d:%e\n", count, E);
				System.out.print("Final W: ");
				printWeights(w);
				System.out.println("训练次数: " + i + "\t误差值: " + E + "\n--------------------------------------------");
				return w;
			}

			System.out.println("On iteration " + i);

			double[] gradient = new double[N];
			for (int k = 0; k < outputs.length; k++)
			{
				gradient = vectorSum(gradient, gradient(w, inputs[k], outputs[k]));
			}

			for (int j = 0; j < N; j++)
			{
				w[j] -= gradient[j];
			}

		}

		System.out.print("Final w: ");
		printWeights(w);
		System.out.println("训练次数: " + Epochs + "\t误差值: " + E + "\n--------------------------------------------");
		return w;
	}

	public double[] TrainingSample(double inputs[][], double outputs[])
	{
		if (inputs == null || inputs.length == 0 || inputs[0].length != this.N || outputs == null || inputs.length != outputs.length)
		{
			return null;
		}

		// 初始化权值
		for (int i = 0; i < this.N + 1; i++)
		{
			W[i] = rand.nextDouble() * 2 - 1;
		}
		System.out.print("Initial W: ");
		System.out.println(Arrays.toString(W));

		// 开始训练
		double Ek = 0, E;
		int count = 0;// 训练次数
		do
		{
			double[] YY = new double[outputs.length];
			for (int i = 0; i < inputs.length; i++)
			{
				double Yi = outputs[i];
				double Y = identify(inputs[i]);
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
				System.out.print("Final W: ");
				printWeights(W);
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
				W[i] -= theta_W[i];
				// W[i] -= theta[i];
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

		System.out.print("Final W: ");
		printWeights(W);
		System.out.println("训练次数: " + count + "\t误差值: " + E + "\n--------------------------------------------");
		return W;
	}

	public static void main(String[] args)
	{
		int l = 2;
		double inputs[][] = new double[][]
		{
				{
						0, 0, 0, 0, 0, 0, 0, 0, 0, 0
				},
				{
						0, 0, 0, 0, 0, 0, 0, 0, 1, 0
				},
				{
						0, 0, 0, 0, 0, 0, 0, 1, 0, 0
				},
				{
						0, 0, 0, 0, 0, 0, 0, 1, 1, 0
				},
				{
						1, 1, 1, 1, 1, 1, 1, 1, 1, 1
				},
				{
						1, 1, 1, 1, 1, 1, 1, 0, 1, 1
				},
				{
						1, 1, 1, 1, 1, 1, 1, 1, 0, 1
				},
				{
						1, 1, 1, 1, 1, 1, 1, 0, 0, 1
				}
		};
		double outputs[] = new double[]
		{
				0, 0, 0, 0, 1, 1, 1, 1
		};

		LogisticRegression lr = new LogisticRegression(10);
		double w[] = lr.TrainingSample(inputs, outputs);
		double input[] = inputs[0];
		double output = lr.identify(input);
		System.out.print("input: ");
		System.out.println(Arrays.toString(input));
		System.out.print("output: ");
		System.out.println(output);
	}
}
