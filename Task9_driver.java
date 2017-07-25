
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class Task9_driver {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		Configuration conf = new Configuration();
		int minimum_sales = Integer.parseInt(args[0]);
		int maximum_sales = Integer.parseInt(args[1]);
		conf.setInt("min_sales", minimum_sales);
		conf.setInt("max_sales", maximum_sales);
		Job job =Job.getInstance(conf, "Sort Tv Sales, custom partitioner with user input max min sales");
		
		job.setJarByClass(Task9_driver.class);
		job.setMapperClass(Task9_mapper.class);
		job.setReducerClass(Task9_reducer.class);
		job.setPartitionerClass(Task9_partitioner.class);
		
		
		
		int num_of_reducers = Integer.parseInt(args[2]);
		
		job.setNumReduceTasks(num_of_reducers);
		
		FileInputFormat.setInputPaths(job, new Path(args[3]));
		FileOutputFormat.setOutputPath(job, new Path(args[4]));
		
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		System.exit(job.waitForCompletion(true) ? 0 :-1);

	}

}

