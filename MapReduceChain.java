package edu.gatech.cse6242;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;

public class Q4 {
	private static final String TEMP_PATH = "difference_temp_output";

  	public static class differenceMapper extends Mapper<Object, Text, IntWritable, IntWritable>{
  		
  		private final static IntWritable plusOne = new IntWritable(1);
    	private final static IntWritable minusOne = new IntWritable(-1);
    	private IntWritable source = new IntWritable();
    	private IntWritable target = new IntWritable();

    	public void map(Object key, Text value, Context context
        	            ) throws IOException, InterruptedException {
      		StringTokenizer itr = new StringTokenizer(value.toString(), "\n");
	      	while (itr.hasMoreTokens()) {
		      	String record[] = itr.nextToken().split("\t");
		        source.set(Integer.parseInt(record[0]));
		        target.set(Integer.parseInt(record[1]));
		        context.write(source, plusOne);
		        context.write(target, minusOne);
	      	}
    	}
  	}	

  	public static class differenceReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
	    
	    private IntWritable result = new IntWritable();
	    public void reduce(IntWritable key, Iterable<IntWritable> values, Context context
	                    ) throws IOException, InterruptedException {
	      	int differenceValue = 0;
	      	for (IntWritable value : values) {
		        differenceValue += value.get();
	      	}
		    result.set(differenceValue);
		    context.write(key, result);
	    }
  	}


  	public static class countMapper extends Mapper<Object, Text, IntWritable, IntWritable>{
    	
    	private IntWritable difference = new IntWritable();
    	private final static IntWritable plusOne = new IntWritable(1);
    	public void map(Object key, Text value, Context context
        	            ) throws IOException, InterruptedException {
    		StringTokenizer itr = new StringTokenizer(value.toString(), "\n");
	      	while (itr.hasMoreTokens()) {
		      	String record[] = itr.nextToken().split("\t");
		      	difference.set(Integer.parseInt(record[1]));
		       	context.write(difference, plusOne);
	      	}
    	}
  	}	

	public static class countReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
	    
	    private IntWritable result = new IntWritable();
	    public void reduce(IntWritable key, Iterable<IntWritable> values, Context context
	                    ) throws IOException, InterruptedException {
	      	int differenceCount = 0;
	      	for (IntWritable value : values) {
		        differenceCount += value.get();
	      	}
		    result.set(differenceCount);
		    context.write(key, result);
	    }
  	}

  	public static void main(String[] args) throws Exception {

    	Configuration conf = new Configuration();
    	Path inputPath = new Path(args[0]);
    	Path tempPath = new Path(TEMP_PATH);
    	Path outputPath = new Path(args[1]);

    	/* first mapreduce to get difference in degree */
    	Job diffJob = Job.getInstance(conf, "difference");
    	diffJob.setJarByClass(Q4.class);
    	FileInputFormat.addInputPath(diffJob, inputPath);
	    FileOutputFormat.setOutputPath(diffJob, tempPath);
    	diffJob.setMapperClass(differenceMapper.class);
    	diffJob.setCombinerClass(differenceReducer.class);
    	diffJob.setReducerClass(differenceReducer.class);
    	diffJob.setOutputKeyClass(IntWritable.class);
	    diffJob.setOutputValueClass(IntWritable.class);
	    
	    if(!diffJob.waitForCompletion(true))
	    	System.exit(1);

	    /* second mapreduce to get difference count */
	    Job countJob = Job.getInstance(conf, "count");
	    countJob.setJarByClass(Q4.class);
	   	FileInputFormat.addInputPath(countJob, tempPath);
	    FileOutputFormat.setOutputPath(countJob, outputPath);
    	countJob.setMapperClass(countMapper.class);
    	countJob.setCombinerClass(countReducer.class);
    	countJob.setReducerClass(countReducer.class);
    	countJob.setOutputKeyClass(IntWritable.class);
	    countJob.setOutputValueClass(IntWritable.class);

	    boolean runningStatus = countJob.waitForCompletion(true);
	    FileSystem configuredFS = FileSystem.get(conf);
	    configuredFS.delete(tempPath, true);
	    System.exit(runningStatus ? 0 : 1);
	}
}