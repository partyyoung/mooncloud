package net.mooncloud.ml;

import java.util.Random;

/**
 * BP（Back
 * Propagation）网络是1986年由Rumelhart和McCelland为首的科学家小组提出，是一种按误差逆传播算法训练的多层前馈网络，是目前应用最广泛的神经网络模型之一。BP网络能学习和存贮大量的输入-输出模式映射关系，而无需事前揭示描述这种映射关系的数学方程。它的学习规则是使用最速下降法，通过反向传播来不断调整网络的权值和阈值
 * ，使网络的误差平方和最小。BP神经网络模型拓扑结构包括输入层（input）、隐层(hide layer)和输出层(output layer)。
 * 
 * @author jiandang
 * 
 */
public class BPNeuralNetwork
{
	private double Goal = 1e-2; // 目标误差
	private long Epochs = 300; // 训练次数

	private double alpha = 0.9; // 0.9~1
	private double eta = 0.1; // 0.1~3

	// 3层网络
	private int Layers = 3; // 层数
	private int InputLayerNums = 70; // 输入层结点数
	private int HiddenLayerNums = 26; // 中间层结点数
	private int OutputLayerNums = 10; // 输出层结点数

	private double W1[][] = new double[HiddenLayerNums][InputLayerNums], W2[][] = new double[OutputLayerNums][HiddenLayerNums]; // 权值
	private double theta_W1[][] = new double[HiddenLayerNums][InputLayerNums], theta_W2[][] = new double[OutputLayerNums][HiddenLayerNums];// 权值修正量

	// 多层网络
	private double Ws[][][] = new double[2][][];

	/**
	 * 默认3层网络
	 * 
	 * @param Goal
	 * @param Epochs
	 * @param alpha
	 * @param eta
	 * @param InputLayerNums
	 * @param HiddenLayerNums
	 * @param OutputLayerNums
	 */
	public BPNeuralNetwork(double Goal, long Epochs, double alpha, double eta, int InputLayerNums, int HiddenLayerNums, int OutputLayerNums)
	{
		this.Goal = Goal;
		this.Epochs = Epochs;
		this.alpha = alpha;
		this.eta = eta;

		this.InputLayerNums = InputLayerNums;
		this.OutputLayerNums = OutputLayerNums;
		this.HiddenLayerNums = HiddenLayerNums;

		W1 = new double[HiddenLayerNums][InputLayerNums]; // 第一层权值
		W2 = new double[OutputLayerNums][HiddenLayerNums]; // 第二层权值
		theta_W1 = new double[HiddenLayerNums][InputLayerNums]; // 第一层权值修正量
		theta_W2 = new double[OutputLayerNums][HiddenLayerNums];// 第二层权值修正量
	}

	public double[] Identify(final double input_Layer[])
	{
		if (input_Layer.length != this.InputLayerNums)
		{
			return null;
		}
		return Identify(input_Layer, new double[this.HiddenLayerNums], new double[this.OutputLayerNums]);
	}

	public double[] Identify(final double input_Layer[], final double Ws[][][])
	{
		if (input_Layer.length != this.InputLayerNums)
		{
			return null;
		}
		W1 = Ws[0];
		W2 = Ws[1];
		return Identify(input_Layer, new double[this.HiddenLayerNums], new double[this.OutputLayerNums]);
	}

	private double[] Identify(final double input_Layer[], double hidden_Layer[], double output_Layer[])
	{
		// 从前向后各层计算各单元
		// 隐层
		int i, j;
		double net_i = 0.0;
		for (i = 0; i < HiddenLayerNums; i++)
		{
			net_i = 0.0;
			for (j = 0; j < InputLayerNums; j++)
			{
				net_i += W1[i][j] * input_Layer[j];
			}
			hidden_Layer[i] = 1 / (1 + Math.exp(-net_i));
		}
		// 输出层
		for (i = 0; i < OutputLayerNums; i++)
		{
			net_i = 0.0;
			for (j = 0; j < HiddenLayerNums; j++)
			{
				net_i += W2[i][j] * hidden_Layer[j];
			}
			output_Layer[i] = 1 / (1 + Math.exp(-net_i));
			// printf("%f ",output_Layer[i]);
		}
		return output_Layer;
	}

	public double[][][] TrainingSample(double inputs[][], double outputs[][])
	{
		if (inputs.length != outputs.length)
		{
			return null;
		}

		int i, j, k, number, count = 0;
		double input_Layer[] = new double[InputLayerNums], hidden_Layer[] = new double[HiddenLayerNums], output_Layer[] = new double[OutputLayerNums];
		double Dp[] = new double[OutputLayerNums], E = 0.0, Ek = 0, D_value, E_w_theta;
		double Theta1[] = new double[HiddenLayerNums], Theta2[] = new double[OutputLayerNums]; // 权值修正值

		// 初始化权值
		Random r = new Random();
		for (i = 0; i < HiddenLayerNums; i++)
		{
			for (j = 0; j < InputLayerNums; j++)
			{
				W1[i][j] = (r.nextDouble()) * 0.6 - 0.3; // ±0.3区间
			}
		}
		for (i = 0; i < OutputLayerNums; i++)
		{
			for (j = 0; j < HiddenLayerNums; j++)
			{
				W2[i][j] = (r.nextDouble()) * 0.6 - 0.3;
			}
		}

		int Number_Count = inputs.length; // 样本个数
		do
		{
			E = 0;
			for (number = 0; number < Number_Count; number++)
			{
				input_Layer = inputs[number];
				Dp = outputs[number];

				// 根据当前权值计算，计算输入数据对应输出数据
				Identify(input_Layer, hidden_Layer, output_Layer);

				// 计算误差E
				D_value = 0.0;
				for (i = 0; i < OutputLayerNums; i++)
				{
					D_value += Math.pow((Dp[i] - output_Layer[i]), 2); // ((Dp[i] - output_Layer[i]) * (Dp[i] - output_Layer[i]));// sqrt平方和
				}
				E += D_value / 2;

				// 计算输出层δ
				for (i = 0; i < OutputLayerNums; i++)
				{
					Theta2[i] = (Dp[i] - output_Layer[i]) * output_Layer[i] * (1 - output_Layer[i]);
				}
				// printf("2计算输出层δ\n");

				// 计算隐层δ
				E_w_theta = 0.0;
				for (i = 0; i < HiddenLayerNums; i++)
				{
					E_w_theta = 0.0;
					for (k = 0; k < OutputLayerNums; k++)
					{
						E_w_theta += Theta2[k] * W2[k][i];
					}
					Theta1[i] = hidden_Layer[i] * (1 - hidden_Layer[i]) * E_w_theta;
				}
				// printf("3计算隐层δ\n");

				// 计算并保存各权值修正值，并修正权值
				for (i = 0; i < OutputLayerNums; i++)
				{
					for (j = 0; j < HiddenLayerNums; j++)
					{
						theta_W2[i][j] = alpha * theta_W2[i][j] + eta * Theta2[i] * hidden_Layer[j];
						W2[i][j] += theta_W2[i][j];
					}
				}
				for (i = 0; i < HiddenLayerNums; i++)
				{
					for (j = 0; j < InputLayerNums; j++)
					{
						theta_W1[i][j] = alpha * theta_W1[i][j] + eta * Theta1[i] * input_Layer[j];
						W1[i][j] += theta_W1[i][j];
					}
				}
				// printf("4计算并保存各权值修正值，修正权值\n");

			}// 样本训练

			count++;// 训练次数自增
//			System.out.println("训练次数: " + count + "\t误差值: " + E + "\n--------------------------------------------");
			// 是否要重新训练
			// 所有样本都训练结束
			if (E < Goal)
			{
				// 达到预定误差
				// printf("\n%d:%e\n", count, E);
				System.out.println("训练次数: " + count + "\t误差值: " + E + "\n--------------------------------------------");
				Ws[0] = W1;
				Ws[1] = W2;
				return Ws;
			}

			// 自适应学习速度， eta-η
			if (Ek > 0)
				if (E < Ek)
				{
					eta = 1.05 * eta;
				}
				else if (E > 1.04 * Ek)
				{
					eta = 0.7 * eta;
				}
			Ek = E;
		}
		while (count < Epochs);
		System.out.println("训练次数: " + count + "\t误差值: " + E + "\n--------------------------------------------");
		return null;
	}

	public static void main(String[] args)
	{
		double inputs[][] = new double[8][8], outputs[][] = new double[8][3];
		for (int i = 0; i < 8; i++)
		{
			inputs[i][i] = 1;
		}
		for (int i = 0; i < 8; i++)
		{
			for (int j = 0; j < 3; j++)
			{
				outputs[i][3 - 1 - j] = ((1 << j) & i) > 0 ? 1 : 0;
			}
		}

		BPNeuralNetwork bpnn = new BPNeuralNetwork(1e-6, 1000, 0.9, 0.1, 8, 64, 8);
		double Ws[][][] = bpnn.TrainingSample(inputs, inputs);

		for (int i = 0; i < Ws.length; i++)
		{
			for (int j = 0; j < Ws[i].length; j++)
			{
				for (int k = 0; k < Ws[i][j].length; k++)
				{
					System.out.print(Ws[i][j][k] + "\t");
				}
				System.out.println();
			}
			System.out.println("--------------------------------------------");
		}

		double input[] = inputs[7];
		double output[] = bpnn.Identify(input, Ws);
		for (int i = 0; i < input.length; i++)
		{
			System.out.print(input[i] + "\t");
		}
		System.out.println();
		for (int i = 0; i < output.length; i++)
		{
			System.out.print(output[i] + "\t");
		}
	}
}
