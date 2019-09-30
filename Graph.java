import java.io.*;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


public class Graph {



   public static class MapperNeighbourCount extends Mapper<Object,Text,IntWritable,IntWritable>{
   	@Override
	   public void map(Object key ,Text value,Context context) throws IOException,InterruptedException {
	   		//String line=value.toString();
			Scanner s=new Scanner(value.toString()).useDelimiter(",");
			int key1=s.nextInt();
			int value1=s.nextInt();
			context.write(new IntWritable(key1),new IntWritable(value1));
			s.close();



   }
}
	public static class ReducerNeighbourCount extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable>{
	@Override
		public void reduce(IntWritable key1,Iterable<IntWritable> values,Context context) throws IOException,InterruptedException{
			int count=0;
				for (IntWritable value : values)
				{
					count+=1;

				}
				context.write(key1,new IntWritable(count));


		}
		}
		    public static void main ( String[] args ) throws Exception {
	Configuration c=new Configuration();
	Job job=Job.getInstance();
	job.setJobName("Neighbour_Job");
	job.setJarByClass(Graph.class);

	job.setMapperClass(MapperNeighbourCount.class);
	job.setReducerClass(ReducerNeighbourCount.class);

	job.setOutputKeyClass(IntWritable.class);
	job.setOutputValueClass(IntWritable.class);

	job.setMapOutputKeyClass(IntWritable.class);
	job.setMapOutputValueClass(IntWritable.class);

	job.setInputFormatClass(TextInputFormat.class);
	job.setOutputFormatClass(TextOutputFormat.class);

	FileInputFormat.setInputPath(job, new Path(args[0]));
	FileOutputFormat.setOutputPath(job,new Path(args[1]));




   }
}
	