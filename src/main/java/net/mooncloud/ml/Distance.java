package net.mooncloud.ml;

public class Distance
{
	/**
	 * <b>曼哈顿距离</b>
	 * <p>
	 * 定义: <code>∑|ai-bi| (i=1,2.n)</code>
	 * 
	 * <pre>
	 * 曼哈顿距离的正式意义为L1-距离或城市区块距离，也就是在欧几里德空间的固定直角坐标系上两点所形成的线段对轴产生的投影的距离总和。
	 * </pre>
	 * 
	 * </p>
	 * 
	 * @param a
	 * @param b
	 * @return
	 */
	public static double ManhattanDistance(double[] a, double[] b)
	{
		double d = 0.0;
		for (int i = 0; i < a.length; i++)
		{
			d += Math.abs(a[i] - b[i]);
		}
		return d;
	}

	/**
	 * <b>欧氏距离</b>
	 * <p>
	 * 定义: <code>√(∑(ai-bi)^2) (i=1,2.n)</code>
	 * 
	 * <pre>
	 * 欧几里得度量（euclidean metric）是一个通常采用的距离定义，指在m维空间中两个点之间的真实距离，或者向量的自然长度（即该点到原点的距离）。在二维和三维空间中的欧氏距离就是两点之间的实际距离。
	 * </pre>
	 * 
	 * </p>
	 * 
	 * @param a
	 * @param b
	 * @return
	 */
	public static double EuclidDistance(double[] a, double[] b)
	{
		double d = 0.0;
		for (int i = 0; i < a.length; i++)
		{
			d += Math.pow(a[i] - b[i], 2);
		}
		return Math.sqrt(d);
	}

	/**
	 * <b>明可夫斯基距离</b>
	 * <p>
	 * 定义: <code>(∑|ai-bi|^p)^(1/p) (i=1,2.n)</code>
	 * 
	 * <pre>
	 * 又称明氏距离，是欧氏空间中的一种测度，被看做是欧氏距离和曼哈顿距离的一种推广。
	 * p取1或2时的明氏距离是最为常用的，p=2即为欧氏距离，而p=1时则为曼哈顿距离。
	 * </pre>
	 * 
	 * </p>
	 * 
	 * @param a
	 * @param b
	 * @param p
	 * @return
	 */
	public static double MinkowskiDistance(double[] a, double[] b, double p)
	{
		double d = 0.0;
		for (int i = 0; i < a.length; i++)
		{
			d += Math.pow(Math.abs(a[i] - b[i]), p);
		}
		return Math.pow(d, 1 / p);
	}

	/**
	 * <b>切比雪夫距离</b>
	 * <p>
	 * 定义: 切比雪夫距离是向量空间中的一种度量，二个点之间的距离定义为其各座标数值差的最大值。以(a1,b1)和(a2,b2)二点为例，其切比雪夫距离为max(|a2-a1|,|b2-b1|)。
	 * </p>
	 * 
	 * @param a
	 * @param b
	 * @return
	 */
	public static double ChebyshevDistance(double[] a, double[] b)
	{
		double d = 0.0;
		for (int i = 0; i < a.length; i++)
		{
			d = Math.max(d, Math.abs(a[i] - b[i]));
		}
		return d;
	}

}
