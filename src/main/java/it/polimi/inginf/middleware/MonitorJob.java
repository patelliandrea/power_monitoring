package it.polimi.inginf.middleware;

import java.io.BufferedReader;
import java.io.InputStreamReader;

import it.polimi.inginf.middleware.mapper.MapperKey;
import it.polimi.inginf.middleware.mapper.MapperValue;
import it.polimi.inginf.middleware.mapper.PowerMonitorMapper;
import it.polimi.inginf.middleware.reducer.PowerMonitorReducer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class MonitorJob extends Configured implements Tool{
	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.printf("Usage: %s [generic options] <input> <output>\n",
					getClass().getSimpleName());
			ToolRunner.printGenericCommandUsage(System.err);
			return -1;
		}
		System.out.println("Start Run");

		Path pt = new Path(args[0]);
		FileSystem fs = FileSystem.get(new Configuration());
		BufferedReader br = new BufferedReader(new InputStreamReader(
				fs.open(pt)));
		String line;
		line = br.readLine();
		System.out.println(line);
		Configuration jobConf = getConf();
		jobConf.set("time", line.split(",")[1]);

		@SuppressWarnings("deprecation")
		Job job = new Job(jobConf, "Median Load");

		//Job job = new Job(getConf(), "Median load");
		job.setJarByClass(getClass());
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(PowerMonitorMapper.class);
		job.setReducerClass(PowerMonitorReducer.class);

		job.setMapOutputKeyClass(MapperKey.class);
		job.setMapOutputValueClass(MapperValue.class);

		job.setOutputKeyClass(MapperKey.class);
		job.setOutputValueClass(FloatWritable.class);

		return job.waitForCompletion(true) ? 0 : 1;
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