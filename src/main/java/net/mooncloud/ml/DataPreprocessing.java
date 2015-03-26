package net.mooncloud.ml;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

public class DataPreprocessing
{
	private static final String delimiting = ",";

	/**
	 * @param file
	 * @param delimiting
	 * @param attrnum
	 * @return
	 * @throws IOException
	 */
	public static double[][] load(File file, String delimiting, int attrnum) throws IOException
	{
		double[][] data;
		ArrayList<String> lines = new ArrayList<String>();

		FileReader fr = new FileReader(file);
		BufferedReader br = new BufferedReader(fr);

		String line = br.readLine();
		while ((line = br.readLine()) != null)
		{
			lines.add(line);
		}

		br.close();
		fr.close();

		data = new double[lines.size()][];
		Iterator<String> iter = lines.iterator();
		int i = 0;
		while (iter.hasNext())
		{
			line = iter.next();
			String[] x = line.split(delimiting, -1);
			if (x.length != attrnum)
				continue;
			data[i] = new double[attrnum];
			for (int j = 0; j < attrnum; j++)
			{
				try
				{
					data[i][j] = Double.parseDouble(x[j]);
				}
				catch (Exception e)
				{
				}
			}
			iter.remove();
		}

		return data;
	}

	public static double[][] normalizationMinMax(double[][] inputs) throws IOException
	{
		double[][] maxiandmini = maximumAndMinimum(inputs);
		double[] maxi = maxiandmini[0], mini = maxiandmini[1];

		int N = inputs.length, M = inputs[0].length;
		double[][] data = new double[N][M];
		for (int i = 0; i < N; i++)
		{
			for (int j = 0; j < M; j++)
			{
				data[i][j] = (inputs[i][j] - mini[j]) / (maxi[j] - mini[j]);
			}
		}
		return data;
	}

	public static double[][] normalizationZscore(double[][] inputs) throws IOException
	{
		double[][] meanandstandard = meanAndStandardDeviation(inputs);
		double[] mean = meanandstandard[0], standard = meanandstandard[1];

		int N = inputs.length, M = inputs[0].length;
		double[][] data = new double[N][M];
		for (int i = 0; i < N; i++)
		{
			for (int j = 0; j < M; j++)
			{
				data[i][j] = (inputs[i][j] - mean[j]) / (standard[j]);
			}
		}
		return data;
	}

	public static double[][] normalization01(double[][] inputs) throws IOException
	{
		double[][] maxiandmini = maximumAndMinimum(inputs);
		double[] maxi = maxiandmini[0], mini = maxiandmini[1];

		int N = inputs.length, M = inputs[0].length;
		double[][] data = new double[N][M];
		for (int i = 0; i < N; i++)
		{
			for (int j = 0; j < M; j++)
			{
				data[i][j] = (inputs[i][j] - mini[j]) / (maxi[j] - mini[j]);
			}
		}
		return data;
	}

	/**
	 * 每一列的最大和最小值
	 * 
	 * @param inputs
	 * @return
	 */
	private static double[][] maximumAndMinimum(double[][] inputs)
	{
		int N = inputs.length, M = inputs[0].length;
		double[][] maxiandmini = new double[2][M];
		for (int j = 0; j < M; j++)
		{
			maxiandmini[0][j] = maxiandmini[1][j] = inputs[0][j];
		}
		for (int i = 1; i < N; i++)
		{
			for (int j = 0; j < M; j++)
			{
				if (maxiandmini[0][j] < inputs[0][j])
				{
					maxiandmini[0][j] = inputs[0][j];
				}
				if (maxiandmini[1][j] > inputs[0][j])
				{
					maxiandmini[1][j] = inputs[0][j];
				}
			}
		}
		return maxiandmini;
	}

	/**
	 * 每一列的均值和标准差
	 * 
	 * @param inputs
	 * @return
	 */
	private static double[][] meanAndStandardDeviation(double[][] inputs)
	{
		int N = inputs.length, M = inputs[0].length;
		double[][] meanandstandard = new double[2][M];
		for (int j = 0; j < M; j++)
		{
			meanandstandard[0][j] = inputs[0][j];
		}
		for (int i = 1; i < N; i++)
		{
			for (int j = 0; j < M; j++)
			{
				meanandstandard[0][j] = meanandstandard[0][j] / ((i + 1.0) / i) + inputs[0][j] / (i + 1);
			}
		}

		for (int i = 0; i < N; i++)
		{
			for (int j = 0; j < M; j++)
			{
				meanandstandard[1][j] += Math.pow(inputs[i][j] - meanandstandard[0][j], 2);
			}
		}
		for (int i = 0; i < M; i++)
		{
			meanandstandard[1][i] = Math.sqrt(meanandstandard[1][i] / N);
		}

		return meanandstandard;
	}
}
