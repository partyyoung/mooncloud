package net.mooncloud.ml;

import java.util.TreeMap;

/**
 * 季节性交乘趋势模型 Y = (at + b) * fi
 * 
 * @author Administrator
 * 
 */
public class SeasonTrendModel
{
	private int seasonalCycle = 12;
	private double[] w;
	private int[] m;
	private double[] Si;

	private double[] ft;

	public int getSeasonalCycle()
	{
		return seasonalCycle;
	}

	public void setSeasonalCycle(int seasonalCycle)
	{
		this.seasonalCycle = seasonalCycle;
	}

	public void mergeSi(SeasonTrendModel other)
	{
		if (other == null || other.Si == null || other.m == null)
		{
			return;
		}
		for (int i = 0; i < seasonalCycle; i++)
		{
			Si[i] += other.Si[i];
			m[i] += other.m[i];
		}
	}

	/**
	 * @param t
	 *            时间向量
	 * @param Yt
	 *            数值向量
	 * @return pYt 预测下一个seasonalCycle的值
	 */
	public double[] seasonCrossMulitiplicationTrendModel(double[] t, double[] Yt)
	{
		if (Yt == null || Yt.length < seasonalCycle)
		{
			return null;
		}

		int num = Yt.length;
		TreeMap<Double, Double> tYt = new TreeMap<Double, Double>();
		double[][] tt = new double[num][1];
		if (t == null)
		{
			t = new double[num];
			for (int i = 0; i < num; i++)
			{
				tt[i][0] = i;
				t[i] = i;
				tYt.put(t[i], Yt[i]);
			}
		}
		else
		{
			for (int i = 0; i < num; i++)
			{
				tt[i][0] = t[i];
				tYt.put(t[i], Yt[i]);
			}
		}
		// 1确定趋势直线方程，利用最小二乘法，y = at + b
		w = LinearRegression.linearRegression(tt, Yt);
		// 计算Yt的趋势值Ft,和季节指数St
		double[] Ft = new double[num];
		double[] St = new double[num];

		Object[] ot = tYt.keySet().toArray();
		for (int i = 0; i < num; i++)
		{
			t[i] = (Double) ot[i];
			Yt[i] = tYt.get(ot[i]);
			Ft[i] = w[0] * t[i] + w[1];
			St[i] = Yt[i] / Ft[i];
		}

		// 2计算季节指数
		m = new int[seasonalCycle];
		Si = new double[seasonalCycle];
		for (int i = 0; i < num; i++)
		{
			Si[i % seasonalCycle] += St[i];
			m[i % seasonalCycle]++;
		}

		ft = new double[seasonalCycle];
		// for (int i = 0; i < seasonalCycle; i++) {
		// int j = (num - 1) - (seasonalCycle - i - 1), m = 0;
		// double Si = 0.0;
		// while (j > 0) {
		// Si += St[j];
		// j -= seasonalCycle;
		// m++;
		// }
		// ft[i] = Si / m;
		// }
		for (int i = 0; i < seasonalCycle; i++)
		{
			ft[i] = Si[i] / m[i];
		}

		// 3预测
		int pt = (int) (t[num - 1] + 1);
		double[] pYt = new double[seasonalCycle];
		for (int i = 0; i < seasonalCycle; i++)
		{
			pYt[i] = (w[0] * pt + w[1]) * ft[pt % seasonalCycle];
			System.out.println(pYt[i]);
			pt++;
		}

		return pYt;
	}

	/**
	 * @param t
	 *            时间向量
	 * @param Yt
	 *            数值向量
	 * @return pYt 预测下一个seasonalCycle的值
	 */
	public double[] seasonSuperpositionTrendModel(double[] t, double[] Yt)
	{
		if (Yt == null || Yt.length < seasonalCycle)
		{
			return null;
		}

		int num = Yt.length;
		TreeMap<Double, Double> tYt = new TreeMap<Double, Double>();
		double[][] tt = new double[num][1];
		if (t == null)
		{
			t = new double[num];
			for (int i = 0; i < num; i++)
			{
				tt[i][0] = i;
				t[i] = i;
				tYt.put(t[i], Yt[i]);
			}
		}
		else
		{
			for (int i = 0; i < num; i++)
			{
				tt[i][0] = t[i];
				tYt.put(t[i], Yt[i]);
			}
		}
		// 1确定趋势直线方程，利用最小二乘法，y = at + b
		w = LinearRegression.linearRegression(tt, Yt);
		// 计算Yt的趋势值Ft,和季节增量Dt
		double[] Ft = new double[num];
		double[] Dt = new double[num];

		Object[] ot = tYt.keySet().toArray();
		for (int i = 0; i < num; i++)
		{
			t[i] = (Double) ot[i];
			Yt[i] = tYt.get(ot[i]);
			Ft[i] = w[0] * t[i] + w[1];
			Dt[i] = Yt[i] - Ft[i];
		}

		// 2计算季节增量

		m = new int[seasonalCycle];
		Si = new double[seasonalCycle];
		for (int i = 0; i < num; i++)
		{
			Si[i % seasonalCycle] += Dt[i];
			m[i % seasonalCycle]++;
		}

		ft = new double[seasonalCycle];
		for (int i = 0; i < seasonalCycle; i++)
		{
			ft[i] = Si[i] / m[i];
		}

		// 3预测
		int pt = (int) (t[num - 1] + 1);
		double[] pYt = new double[seasonalCycle];
		for (int i = 0; i < seasonalCycle; i++)
		{
			pYt[i] = w[0] * pt + w[1] + ft[pt % seasonalCycle];
			// System.out.println(pYt[i]);
			pt++;
		}

		return pYt;
	}

	public static void main(String[] args)
	{
		double[] t =
		{ 21.0, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33 };
		double[] Yt =
		{ 9267.65, 9791.13, 428, 9732, 10580, 9868, 17220.46, 10470.6, 8015.93, 4474, 13504.6, 12072, 9842.8 };
		SeasonTrendModel model = new SeasonTrendModel();
		model.setSeasonalCycle(7);
		model.seasonCrossMulitiplicationTrendModel(null, Yt);
		System.out.println();
		model.seasonSuperpositionTrendModel(null, Yt);
	}
}
