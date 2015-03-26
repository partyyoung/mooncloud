package net.mooncloud.ml;

import java.io.IOException;

/**
 * 线性相关系数
 * 
 * @author jiandang
 * 
 */
public class CorrelationCoefficient
{
	/**
	 * 线性相关系数强度等级
	 * 
	 * @author Administrator
	 * 
	 */
	public enum CorrelationStatus
	{
		NEGATIVE_VERYSTRONG(-8), NEGATIVE_STRONG(-6), NEGATIVE_MIDDLING(-4), NEGATIVE_WEAK(-2), NEGATIVE(-1), NON(0), POSITIVE(1), WEAK(2), MIDDLING(4), STRONG(6), VERYSTRONG(8);
		private CorrelationStatus(int i)
		{
			rank = i;
		}

		private int rank;

		public int getRank()
		{
			return rank;
		}
	}

	public static CorrelationStatus corrStatusDetail;
	public static CorrelationStatus corrStatus;

	/**
	 * 线性相关系数 n*sum(x*y)-sum(x)*sum(y))/sqrt((n*sum(x^2)-sum(x)^2)*(n*sum(y^2)-sum (y)^2)
	 * <p>
	 * 0.8~1.0 非常强的相关, 0.6~0.8 强相关, 0.4~0.6 中度相关, 0.2~0.4 弱相关, 0~0.2 弱相关或无相关
	 * 
	 * @param x
	 * @param y
	 * @return
	 * @throws IOException
	 */
	public static double linearCorrelationCoefficient(double[] x, double[] y) throws IOException
	{
		double c = 0.0;
		if (x == null || y == null || x.length <= 0 || y.length <= 0)
		{
			throw (IOException) new IOException("Illegal data");
		}
		if (x.length != y.length)
		{
			throw (IOException) new IOException("Illegal data: x.length:" + x.length + " != y.length:" + y.length);
		}
		int n = x.length;
		c = (n * sum(mul(x, y)) - sum(x) * sum(y)) / Math.sqrt((n * sum(mul(x, x)) - Math.pow(sum(x), 2))) / Math.sqrt((n * sum(mul(y, y)) - Math.pow(sum(y), 2)));

		if (c > 0)
		{
			corrStatus = CorrelationStatus.POSITIVE;
			if (c <= 0.2)
			{
				corrStatusDetail = CorrelationStatus.NON;
			}
			else if (c <= 0.4)
			{
				corrStatusDetail = CorrelationStatus.WEAK;
			}
			else if (c <= 0.6)
			{
				corrStatusDetail = CorrelationStatus.MIDDLING;
			}
			else if (c <= 0.8)
			{
				corrStatusDetail = CorrelationStatus.STRONG;
			}
			else
			{
				corrStatusDetail = CorrelationStatus.VERYSTRONG;
			}
		}
		else if (c < 0)
		{
			corrStatus = CorrelationStatus.NEGATIVE;
			if (c >= -0.2)
			{
				corrStatusDetail = CorrelationStatus.NON;
			}
			else if (c >= -0.4)
			{
				corrStatusDetail = CorrelationStatus.NEGATIVE_WEAK;
			}
			else if (c >= -0.6)
			{
				corrStatusDetail = CorrelationStatus.NEGATIVE_MIDDLING;
			}
			else if (c >= -0.8)
			{
				corrStatusDetail = CorrelationStatus.NEGATIVE_STRONG;
			}
			else
			{
				corrStatusDetail = CorrelationStatus.NEGATIVE_VERYSTRONG;
			}
		}
		else
		{
			corrStatus = CorrelationStatus.NON;
			corrStatusDetail = CorrelationStatus.NON;
		}

		return c;
	}

	public static double[] mul(double[] x, double[] y) throws IOException
	{
		if (x == null || y == null || x.length <= 0 || y.length <= 0)
		{
			throw (IOException) new IOException("Illegal data");
		}
		if (x.length != y.length)
		{
			throw (IOException) new IOException("Illegal data: x.length:" + x.length + " != y.length:" + y.length);
		}
		int n = x.length;
		double[] mul = new double[n];
		for (int i = 0; i < n; i++)
		{
			mul[i] = x[i] * y[i];
		}

		return mul;
	}

	public static double[] add(double[] x, double[] y) throws IOException
	{
		if (x == null || y == null || x.length <= 0 || y.length <= 0)
		{
			throw (IOException) new IOException("Illegal data");
		}
		if (x.length != y.length)
		{
			throw (IOException) new IOException("Illegal data: x.length:" + x.length + " != y.length:" + y.length);
		}
		int n = x.length;
		double[] sum = new double[n];
		for (int i = 0; i < n; i++)
		{
			sum[i] = x[i] + y[i];
		}
		return sum;
	}

	public static double sum(double[] array)
	{
		if (array == null || array.length == 0)
		{
			return 0.0;
		}
		double sum = 0.0;
		for (double x : array)
		{
			sum += x;
		}
		return sum;
	}

	public static double mean(double[] array)
	{
		if (array == null || array.length == 0)
		{
			return 0.0;
		}
		int n = array.length;
		double sum = 0.0;
		for (double x : array)
		{
			sum += x / n;
		}
		return sum;
	}

	public static double mean(float[] array)
	{
		if (array == null || array.length == 0)
		{
			return 0.0;
		}
		int n = array.length;
		double sum = 0.0;
		for (double x : array)
		{
			sum += x / n;
		}
		return sum;
	}

	public static double mean(long[] array)
	{
		if (array == null || array.length == 0)
		{
			return 0.0;
		}
		double n = array.length;
		double sum = 0.0;
		for (long x : array)
		{
			sum += x / n;
		}
		return sum;
	}

	public static double mean(int[] array)
	{
		if (array == null || array.length == 0)
		{
			return 0.0;
		}
		double n = array.length;
		double sum = 0.0;
		for (long x : array)
		{
			sum += x / n;
		}
		return sum;
	}

	public static void main(String[] args) throws IOException
	{
		double[] a =
		{ 1, 2, 3, 4, 5 };
		double[] b =
		{ -1.0, -1.5, -1.0, -1.5, -3.0 };
		double[] c =
		{ 1, 4, 9, 16, 25 };
		System.out.println(linearCorrelationCoefficient(b, mul(c, c)));
		System.out.println(CorrelationCoefficient.corrStatus.getRank());
		System.out.println(CorrelationCoefficient.corrStatusDetail);
		System.out.println(CorrelationStatus.NON.ordinal());
		System.out.println(CorrelationStatus.NON.compareTo(CorrelationStatus.NEGATIVE_VERYSTRONG));
		System.out.println(CorrelationStatus.NON.compareTo(CorrelationStatus.POSITIVE));
	}
}
