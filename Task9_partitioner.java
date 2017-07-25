import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class Task9_partitioner extends Partitioner<IntWritable,Text> implements Configurable{
	
	private Configuration conf;
	
	@Override
	public int getPartition(IntWritable key, Text value, int num_reducer) {
		
		int min = conf.getInt("min_sales", 0);
		int max = conf.getInt("max_sales", 10);
		int mid = max/2;
		
		int sales = key.get();
		
		if(sales >= min && sales < mid)
		{
			return 0;
		}
		else
		{
			return 1;
		}
	}

	@Override
	public Configuration getConf() {
		return conf;
	}

	@Override
	public void setConf(Configuration arg0) {
		this.conf = arg0;
	}


}
