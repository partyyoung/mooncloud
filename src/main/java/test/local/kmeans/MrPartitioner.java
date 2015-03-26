package test.local.kmeans;

import net.mooncloud.io.Tuple;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;


public class MrPartitioner extends Partitioner<Tuple, Tuple>
{
	@Override
	public int getPartition(Tuple key, Tuple value, int numPartitions)
	{
		// TODO Partitioner
		return (((Text) key.get(0)).hashCode() & Integer.MAX_VALUE) % numPartitions;
	}
}