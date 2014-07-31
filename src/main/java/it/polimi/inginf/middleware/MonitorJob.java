package it.polimi.inginf.middleware;

import it.polimi.inginf.middleware.firstJob.mapper.FirstMapper;
import it.polimi.inginf.middleware.firstJob.mapper.FirstMapperActualKeyGroupingComparator;
import it.polimi.inginf.middleware.firstJob.mapper.FirstMapperActualKeyPartitioner;
import it.polimi.inginf.middleware.firstJob.mapper.FirstMapperCompositeKey;
import it.polimi.inginf.middleware.firstJob.mapper.FirstMapperCompositeKeyComparator;
import it.polimi.inginf.middleware.firstJob.mapper.FirstMapperValue;
import it.polimi.inginf.middleware.firstJob.reducer.FirstReducer;
import it.polimi.inginf.middleware.firstJob.reducer.FirstReducerKey;
import it.polimi.inginf.middleware.firstJob.reducer.FirstReducerValue;
import it.polimi.inginf.middleware.secondJob.mapper.SecondMapper;
import it.polimi.inginf.middleware.secondJob.mapper.SecondMapperActualKeyGroupingComparator;
import it.polimi.inginf.middleware.secondJob.mapper.SecondMapperActualKeyPartitioner;
import it.polimi.inginf.middleware.secondJob.mapper.SecondMapperCompositeKey;
import it.polimi.inginf.middleware.secondJob.mapper.SecondMapperCompositeKeyComparator;
import it.polimi.inginf.middleware.secondJob.mapper.SecondMapperValue;
import it.polimi.inginf.middleware.secondJob.reducer.SecondReducer;
import it.polimi.inginf.middleware.secondJob.reducer.SecondReducerKey;
import it.polimi.inginf.middleware.secondJob.reducer.SecondReducerValue;
import it.polimi.inginf.middleware.thirdJob.mapper.ThirdMapper;
import it.polimi.inginf.middleware.thirdJob.mapper.ThirdMapperKey;
import it.polimi.inginf.middleware.thirdJob.mapper.ThirdMapperValue;
import it.polimi.inginf.middleware.thirdJob.reducer.ThirdReducer;
import it.polimi.inginf.middleware.thirdJob.reducer.ThirdReducerKey;
import it.polimi.inginf.middleware.thirdJob.reducer.ThirdReducerValue;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class MonitorJob extends Configured implements Tool{
	public int run(String[] args) throws Exception {
		Job firstJob = Job.getInstance(getConf(), "job1");
		firstJob.setJarByClass(getClass());
		String firstjoboutput = "firstjoboutput";
		FileInputFormat.addInputPath(firstJob, new Path(args[0]));
		FileOutputFormat.setOutputPath(firstJob, new Path(firstjoboutput));
		
		firstJob.setMapperClass(FirstMapper.class);
		firstJob.setMapOutputKeyClass(FirstMapperCompositeKey.class);
		firstJob.setMapOutputValueClass(FirstMapperValue.class);
		firstJob.setPartitionerClass(FirstMapperActualKeyPartitioner.class);
		firstJob.setGroupingComparatorClass(FirstMapperActualKeyGroupingComparator.class);
		firstJob.setSortComparatorClass(FirstMapperCompositeKeyComparator.class);
		
		firstJob.setReducerClass(FirstReducer.class);
		firstJob.setOutputKeyClass(FirstReducerKey.class);
		firstJob.setOutputValueClass(FirstReducerValue.class);
		
		firstJob.waitForCompletion(true);
		
		Job secondJob = Job.getInstance(getConf(), "job2");
		secondJob.setJarByClass(getClass());
		
		secondJob.setMapperClass(SecondMapper.class);
		secondJob.setMapOutputKeyClass(SecondMapperCompositeKey.class);
		secondJob.setMapOutputValueClass(SecondMapperValue.class);
		secondJob.setPartitionerClass(SecondMapperActualKeyPartitioner.class);
		secondJob.setGroupingComparatorClass(SecondMapperActualKeyGroupingComparator.class);
		secondJob.setSortComparatorClass(SecondMapperCompositeKeyComparator.class);
		
		secondJob.setReducerClass(SecondReducer.class);
		secondJob.setOutputKeyClass(SecondReducerKey.class);
		secondJob.setOutputValueClass(SecondReducerValue.class);
		
		FileInputFormat.addInputPath(secondJob, new Path(firstjoboutput));
	    String secondjoboutput = "secondjoboutput";
	    FileOutputFormat.setOutputPath(secondJob, new Path(secondjoboutput));
	    
	    secondJob.waitForCompletion(true);
	   
	    Job thirdJob = Job.getInstance(getConf(), "job3");
	    thirdJob.setJarByClass(getClass());
	    
	    thirdJob.setMapperClass(ThirdMapper.class);
	    thirdJob.setMapOutputKeyClass(ThirdMapperKey.class);
	    thirdJob.setMapOutputValueClass(ThirdMapperValue.class);
	    
	    thirdJob.setReducerClass(ThirdReducer.class);
	    thirdJob.setOutputKeyClass(ThirdReducerKey.class);
	    thirdJob.setOutputValueClass(ThirdReducerValue.class);
	    
	    FileInputFormat.addInputPath(thirdJob, new Path(secondjoboutput));
	    FileOutputFormat.setOutputPath(thirdJob, new Path(args[1]));
	    
		return thirdJob.waitForCompletion(true) ? 0 : 1;
	}
	
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new MonitorJob(), args);
		System.exit(exitCode);
	}
}

/*
job.setMapOutputKeyClass(CompositeKey.class);
job.setPartitionerClass(ActualKeyPartitioner.class);
job.setGroupingComparatorClass(ActualKeyGroupingComparator.class);
job.setSortComparatorClass(CompositeKeyComparator.class);
*/