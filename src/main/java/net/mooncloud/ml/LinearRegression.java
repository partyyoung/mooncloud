package net.mooncloud.ml;

import java.util.Random;

/**
 * 多元一次线性回归
 * 
 * @author Administrator
 * 
 */
public class LinearRegression
{

	public double[][] getFactor()
	{
		return factor;
	}

	public void setFactor(double[][] factor)
	{
		this.factor = factor;
	}

	/**
	 * 方程组系数矩阵
	 */
	private double[][] factor;

	/**
	 * 根据最小二乘法计算权值的方程组，对称数组
	 * 
	 * @param x
	 *            自变量值向量
	 * @param y
	 *            因变量值
	 * @return factor 权值的方程组
	 */
	public double[][] buildEquationSystem(double[] x, double y)
	{
		if (x == null || x.length < 1)
		{
			return null;
		}

		int num = x.length;
		// 根据最小二乘法计算权值的方程组，对称数组
		if (factor == null)
		{
			factor = new double[num + 1][num + 2];
		}

		for (int j = 0; j < num; j++)
		{
			for (int k = j; k < num; k++)
			{
				double t = x[j] * x[k];
				factor[j][k] += t;
				factor[k][j] = factor[j][k];
			}
			factor[j][num] += x[j] * 1;
			factor[num][j] = factor[j][num];

			factor[j][num + 1] += x[j] * y;
		}
		factor[num][num] += 1;
		factor[num][num + 1] += y;

		return factor;
	}

	/**
	 * 根据最小二乘法计算权值的方程组，对称数组--map
	 * 
	 * @param x
	 *            自变量值向量
	 * @param y
	 *            因变量值
	 * @return factor 权值的方程组
	 */
	public double[][] buildEquationSystem(double[][] x, double[] y)
	{
		if (x == null || x.length < 1 || x[0] == null || x[0].length < 1)
		{
			return null;
		}

		for (int i = 0; i < x.length; i++)
		{
			buildEquationSystem(x[i], y[i]);
		}

		return factor;
	}

	/**
	 * 合并方程组系数 --combine --reduce
	 * 
	 * @param other
	 * @return
	 */
	public double[][] mergeEquationSystem(LinearRegression other)
	{
		if (other == null || other.factor == null)
		{
			return this.factor;
		}

		if (factor == null)
		{
			return factor = other.factor;
		}

		int num = factor.length - 1;

		for (int j = 0; j < num; j++)
		{
			for (int k = j; k < num; k++)
			{
				factor[j][k] += other.factor[j][k];
				factor[k][j] = factor[j][k];
			}
			factor[j][num] += other.factor[j][num];
			factor[num][j] = factor[j][num];
			factor[j][num + 1] += other.factor[j][num + 1];
		}
		factor[num][num] += 1;
		factor[num][num + 1] += other.factor[num][num + 1];

		return factor;
	}

	/**
	 * 权值向量
	 */
	private double[] W;

	/**
	 * 计算权值-解方程组 --reduce
	 * 
	 * @return w 权值向量，最后一个元素为常数项
	 */
	public double[] solveEquationSystem()
	{
		if (factor == null || factor.length < 1)
		{
			return null;
		}

		// 初始化权值向量
		double[] w = new double[factor.length];

		// 化简方程组
		for (int i = 0; i < factor.length - 1; i++)
		{
			double b = factor[i][i];
			for (int k = i + 1; k < factor.length; k++)
			{
				double c = factor[k][i];
				double d = c / b;
				for (int j = i; j < factor[k].length; j++)
				{
					factor[k][j] = factor[k][j] - d * factor[i][j];
				}
			}
		}

		// 计算权值-解方程组
		for (int i = w.length - 1; i >= 0; i--)
		{
			double t = 0f;
			for (int j = i + 1; j < w.length; j++)
			{
				t += factor[i][j] / factor[i][i] * w[j];
			}
			w[i] = (factor[i][w.length] / factor[i][i] - t);
		}

		this.W = w;
		return w;
	}

	// ------------------

	/**
	 * 计算数据向量的斜率
	 * 
	 * @param y
	 *            数据数组
	 * @return k 直线斜率
	 */
	public static double getLineSlope(double[] y)
	{
		int num = y.length;
		double[][] x = new double[num][1];
		for (int i = 0; i < num; i++)
		{
			x[i][0] = i;
		}
		double[] w = linearRegression(x, y);
		return w[0];
	}

	public static double getLineSlope(double[] x, double[] y)
	{
		int num = y.length;
		double[][] xx = new double[num][1];
		for (int i = 0; i < num; i++)
		{
			xx[i][0] = x[i];
		}
		double[] w = linearRegression(xx, y);
		return w[0];
	}

	/**
	 * 多元一次线性回归,JAVA单机版
	 * 
	 * @param x
	 *            自变量样本矩阵
	 * @param y
	 *            因变量样本向量
	 * @return 权值向量，最后一个元素为常数项
	 */
	public static double[] linearRegression(double[][] x, double[] y)
	{
		// TODO JAVA源码
		if (x == null || x.length < 1 || x[0] == null || x[0].length < 1)
		{
			return null;
		}

		int num = x[0].length;
		// 初始化权值向量
		double[] w = new double[num + 1];

		// 根据最小二乘法计算权值的方程组，对称数组
		double[][] factor = new double[num + 1][num + 2];
		for (int i = 0; i < x.length; i++)
		{
			for (int j = 0; j < x[i].length; j++)
			{
				for (int k = j; k < x[i].length; k++)
				{
					double t = x[i][j] * x[i][k];
					factor[j][k] += t;
					factor[k][j] = factor[j][k];
				}
				factor[j][num] += x[i][j] * 1;
				factor[num][j] = factor[j][num];
				factor[j][num + 1] += x[i][j] * y[i];
			}
			factor[num][num] += 1;
			factor[num][num + 1] += y[i];
		}

		// 打印系数矩阵
		// for (int i = 0; i < factor.length; i++)
		// {
		// for (int j = 0; j < factor[i].length; j++)
		// {
		// System.out.print(factor[i][j] + " ");
		// }
		// System.out.println();
		// }

		// 化简方程组
		for (int i = 0; i < factor.length - 1; i++)
		{
			double b = factor[i][i];
			for (int k = i + 1; k < factor.length; k++)
			{
				double c = factor[k][i];
				double d = c / b;
				for (int j = i; j < factor[k].length; j++)
				{
					factor[k][j] = factor[k][j] - d * factor[i][j];
				}
			}
		}

		// 打印系数矩阵
		// System.out.println();
		// for (int i = 0; i < factor.length; i++)
		// {
		// for (int j = 0; j < factor[i].length; j++)
		// {
		// System.out.print(factor[i][j] + " ");
		// }
		// System.out.println();
		// }

		// 计算权值-解方程组
		for (int i = w.length - 1; i >= 0; i--)
		{
			double t = 0f;
			for (int j = i + 1; j < w.length; j++)
			{
				t += factor[i][j] / factor[i][i] * w[j];
			}
			w[i] = (factor[i][w.length] / factor[i][i] - t);
		}

		// 打印权值
		// System.out.println();
		// for (int i = 0; i < w.length; i++)
		// {
		// System.out.print(w[i] + " ");
		// }
		// System.out.println();

		return w;
	}

	/**
	 * 预测
	 * 
	 * @param input
	 * @param W
	 * @return
	 */
	public double Identify(final double input[], final double[] W)
	{
		double z = W[input.length];
		for (int i = 0; i < input.length; i++)
		{
			z += W[i] * input[i];
		}

		return z;// 1 / (1 + Math.pow(Math.E, -z));
	}

	public double Identify(final double input[])
	{
		double z = this.W[input.length];
		for (int i = 0; i < input.length; i++)
		{
			z += this.W[i] * input[i];
		}

		return z;// 1 / (1 + Math.pow(Math.E, -z));
	}

	/**
	 * 回归样本
	 * 
	 * @param x
	 * @param y
	 * @return
	 */
	public double[] TrainingSample(double[][] x, double[] y)
	{
		return this.W = linearRegression(x, y);
	}

	public static void main(String[] args)
	{
		// 模拟y=x
		int n = 10;
		double inputs[][] = new double[n][1];
		double outputs[] = new double[n];
		Random r = new Random();
		for (int i = 0; i < n; i++)
		{
			inputs[i][0] = i;
			outputs[i] = inputs[i][0] + (r.nextDouble() * 0.1 - 0.05);
		}

		LinearRegression lr = new LinearRegression();
		double W[] = lr.TrainingSample(inputs, outputs);

		for (int i = 0; i < W.length; i++)
		{
			System.out.print(W[i] + "\t");
		}
		System.out.println("--------------------------------------------");

		double input[] = inputs[0];
		double output = lr.Identify(input, W);
		for (int i = 0; i < input.length; i++)
		{
			System.out.print(input[i] + "\t");
		}
		System.out.println();
		System.out.print(output + "\t");

	}
}
