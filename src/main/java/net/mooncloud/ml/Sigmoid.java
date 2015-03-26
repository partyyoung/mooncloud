package net.mooncloud.ml;

public class Sigmoid
{
	/**
	 * sigmoid函数: y = c/(1+e^(-1*a*(x-b)))
	 * 
	 * @param x
	 * @param a
	 * @param b
	 * @param c
	 * @return
	 */
	public static double sigmoid(double x, double a, double b, double c)
	{
		if (x == 0 || x - b == 0)
			return 0.5 * c;
		double t = x;
		double fold = 0;
		while (t > 1)
		{
			fold++;
			t = t / 10;
		}
		fold = 10 - fold;
		fold = fold < 0 ? 0 : fold * fold * fold * fold;
		return c / (1 + Math.E * (1 / 0.75 - 1) * Math.pow(Math.E, -1 / (a / (fold + 1) + x / (fold + 1) * fold) * (x - b)));

	}

	public static double sigmoid(double x, double mean, double range)
	{
		return range / (1 + Math.exp(-1 * Math.log(1 + 2 * Math.pow(x / mean, Math.log10(4)))));
	}

	public static double sigmoid(double x)
	{
		return 1 / (1 + Math.pow(Math.E, -x));
		// return sigmoid(x, 1, 0, 1);
	}

	public static void main(String[] args)
	{
		System.out.println(sigmoid(1, 1, 0, 100));
		System.out.println(0 + "\t" + sigmoid(0, 3827339, 0, 100));
		for (int i = 1; i <= 1000000000; i *= 10)
		{
			// System.out.println(3.8273 * i + "\t" + sigmoid(3.827339 * i, 3827339, 0, 100));
			System.out.println(3.8273 * i + "\t" + sigmoid(3.827339 * i, 3827339, 100));
		}
		// System.out.println(sigmoid(3, 3827339, 0, 100));
		// System.out.println(sigmoid(38, 3827339, 0, 100));
		// System.out.println(sigmoid(382, 3827339, 0, 100));
		// System.out.println(sigmoid(3827, 3827339, 0, 100));
		// System.out.println(sigmoid(38273, 3827339, 0, 100));
		// System.out.println(sigmoid(382733, 3827339, 0, 100));
		// System.out.println(sigmoid(3827339, 3827339, 0, 100));
		// System.out.println(sigmoid(38273399, 3827339, 0, 100));
		// System.out.println(sigmoid(38273399 * 3, 3827339, 0, 100));
		// System.out.println(sigmoid(382733990, 3827339, 0, 100));
	}
}
