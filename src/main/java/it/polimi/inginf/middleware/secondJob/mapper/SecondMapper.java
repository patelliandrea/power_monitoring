package it.polimi.inginf.middleware.secondJob.mapper;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SecondMapper extends Mapper<LongWritable, Text, SecondMapperCompositeKey, SecondMapperValue> {
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		line = line.replaceAll("\t", "");
		String[] parts = line.split(",");
		
		int id = Integer.parseInt(parts[0]);
		int idHouse = Integer.parseInt(parts[1]);
		int hour = Integer.parseInt(parts[2]);
		int medianHouse = Integer.parseInt(parts[3]);
		int idHousehold = Integer.parseInt(parts[4]);
		int idPlug = Integer.parseInt(parts[5]);
		int measure = Integer.parseInt(parts[6]);
		context.write(
				new SecondMapperCompositeKey(idHouse, hour, idHousehold, idPlug, measure, medianHouse),
				new SecondMapperValue(id, measure));
	}
}
