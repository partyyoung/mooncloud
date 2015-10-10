package net.mooncloud.ml;

import java.util.Random;

/**
 * KMeans
 * 
 * @author jiandang
 * 
 */
public class KMeans
{
	private double Goal = 1e-9; // 目标误差
	private int K = 2;

	private double[][] kmeans;

	public KMeans()
	{
	}

	public KMeans(int K)
	{
		this.K = K;
	}

	public int Identify(final double input[], final double[][] kmeans)
	{
		// 计算样本与每个中心点的距离,并归类 --map
		double mindistance = Double.MAX_VALUE, distance;
		int c = -1;
		for (int j = 0; j < this.K; j++)
		{
			distance = Distance(input, kmeans[j]);
			if (distance < mindistance)
			{
				mindistance = distance;
				c = j;
			}
		}
		return c;
	}

	public int Identify(final double input[])
	{
		return Identify(input, this.kmeans);
	}

	public int[] TrainingSample(double[][] inputs, double[][] initkmeans)
	{
		this.kmeans = initkmeans;

		int num = inputs.length; // 样本量
		int attrnum = inputs[0].length; // 样本维度
		int[] inputClass = new int[num]; // 样本分类[0, K-1]

		int count = 1;
		do
		{
			// 2.根据K个中心点分类样本样本数据集
			double[][] kmeans2 = new double[this.K][attrnum]; // 更新K个中心点
			int[] classInputs = new int[this.K]; // 样本分类[0, K-1]包含的样本个数
			for (int i = 0; i < num; i++)
			{
				double[] input = inputs[i];// 第i个样本
				// 2.1 计算第i个样本与每个中心点的距离,并归类 --map
				int c = inputClass[i] = Identify(input);
				// 2.2 更新类别c的中心点 --combine--reduce
				classInputs[c]++;
				for (int k = 0; k < attrnum; k++)
				{
					kmeans2[c][k] = (kmeans2[c][k] * (classInputs[c] - 1) + input[k]) / classInputs[c];
				}
			}

			// 3.计算准则函数-新的中心点是否变化 --reduce
			boolean changed = Criterion(kmeans, kmeans2);

			// 4.满足阈值
			if (changed == false)
			{
				break;
			}
			count++;
		}
		while (true);

		System.out.println("训练次数: " + count + "\t误差值: \n--------------------------------------------");
		return inputClass;
	}

	public int[] TrainingSample(double[][] inputs)
	{
		int num = inputs.length; // 样本量

		// 1.随机K个不相同的样本作为初始中心点
		double[][] initkmeans = new double[this.K][]; // K个中心点
		Random r = new Random();
		int SEED = num / this.K;
		for (int i = 0; i < this.K; i++)
		{
			int k = r.nextInt(SEED) + i * SEED;
			initkmeans[i] = inputs[k];
		}

		return TrainingSample(inputs, initkmeans);
	}

	/**
	 * 准则函数
	 * 
	 * @param kmeans
	 * @param kmeans2
	 * @return
	 */
	private boolean Criterion(double[][] kmeans, double[][] kmeans2)
	{
		boolean changed = false;
		for (int i = 0; i < kmeans.length; i++)
		{
			for (int j = 0; j < kmeans[i].length; j++)
			{
				changed |= Math.abs(kmeans2[i][j] - kmeans[i][j]) < Goal ? false : true;
				kmeans[i][j] = kmeans2[i][j];
			}
		}
		return changed;
	}

	/**
	 * 距离函数
	 * 
	 * @param a
	 * @param b
	 * @return
	 */
	private double Distance(double[] a, double[] b)
	{
		return DistanceMeasure.EuclidDistance(a, b);
	}

	public static void main(String[] args)
	{
		KMeans kmeans = new KMeans(4);
		double[][] inputs = new double[20][2];
		Random r = new Random();
		for (int i = 0; i < 20; i++)
		{
			inputs[i][0] = r.nextDouble() * 2 - 1 + r.nextInt(2) * 20;
			inputs[i][1] = r.nextDouble() * 2 - 1 + r.nextInt(2) * 20;
			System.out.println(inputs[i][0] + "\t" + inputs[i][1]);
		}

		int[] inputClass = kmeans.TrainingSample(inputs);

		for (int i = 0; i < 20; i++)
		{
			System.out.println(inputs[i][0] + "\t" + inputs[i][1] + " \t" + inputClass[i]);
		}
		System.out.println("--------------------------------------------");

		System.out.println(kmeans.Identify(new double[]
		{
				20, 20
		}));
	}
}
