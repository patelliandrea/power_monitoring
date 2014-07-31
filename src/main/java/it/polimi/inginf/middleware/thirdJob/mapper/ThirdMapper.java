package it.polimi.inginf.middleware.thirdJob.mapper;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ThirdMapper extends Mapper<LongWritable, Text, ThirdMapperKey, ThirdMapperValue> {
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		line = line.replaceAll("\t", "");
		String[] parts = line.split(",");
		System.out.println(line);
		int idHouse = Integer.parseInt(parts[0]);
		int hour = Integer.parseInt(parts[3]);
		int medianPlug = Integer.parseInt(parts[4]);
		int medianHouse = Integer.parseInt(parts[5]);
		context.write(
				new ThirdMapperKey(idHouse, hour),
				new ThirdMapperValue(medianHouse, medianPlug));
	}
}
