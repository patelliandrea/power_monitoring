package it.polimi.inginf.middleware.mapper;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PowerMonitorMapper extends Mapper<LongWritable, Text, MapperKey, MapperValue> {
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		String[] parts = line.split(",");
		
		if(parts.length < 6) {
			return;
		}
		
		long timestamp = Long.parseLong(parts[1]);
		int idPlug = Integer.parseInt(parts[2]);
		int idHouse = Integer.parseInt(parts[4]);
		int measure = Integer.parseInt(parts[5]);
		//Date date = new Date(timestamp * 1000);
		//DateFormat df = new SimpleDateFormat("hh");
		//df.setTimeZone(TimeZone.getTimeZone("GMT"));
		//int hour = Integer.parseInt(df.format(date));
		long hour = (int) (timestamp/3600);
		// map as { idHouse, hour }, { idPlug, measure }
		context.write(new MapperKey(idHouse, hour), new MapperValue(idPlug, measure));
	}
}