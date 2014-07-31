package it.polimi.inginf.middleware.firstJob.mapper;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FirstMapper extends Mapper<LongWritable, Text, FirstMapperCompositeKey, FirstMapperValue> {
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		String[] parts = line.split(",");
		
		int id = Integer.parseInt(parts[0]);
		long timestamp = Long.parseLong(parts[1]);
		int idPlug = Integer.parseInt(parts[2]);
		int idHousehold = Integer.parseInt(parts[3]);
		int idHouse = Integer.parseInt(parts[4]);
		int measure = Integer.parseInt(parts[5]);
		Date date = new Date(timestamp * 1000);
		//System.out.println(date);
		DateFormat df = new SimpleDateFormat("hh");
		df.setTimeZone(TimeZone.getTimeZone("GMT"));
		int hour = Integer.parseInt(df.format(date));
		//System.out.println(hour);
		context.write(new FirstMapperCompositeKey(idHouse, hour, measure), new FirstMapperValue(id, idPlug, idHousehold, measure));
	}
}