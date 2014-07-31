package it.polimi.inginf.middleware.thirdJob.reducer;

import it.polimi.inginf.middleware.thirdJob.mapper.ThirdMapperKey;
import it.polimi.inginf.middleware.thirdJob.mapper.ThirdMapperValue;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Reducer;

public class ThirdReducer extends Reducer<ThirdMapperKey, ThirdMapperValue, ThirdReducerKey, ThirdReducerValue>{
	@Override
	protected void reduce(ThirdMapperKey key, Iterable<ThirdMapperValue> values, Context context) throws IOException, InterruptedException {
		int total = 0;
		int count = 0;
		for(ThirdMapperValue value : values) {
			total++;
			if(value.getMedianPlug() > value.getMedianHouse()) {
				count++;
			}
		}
		System.out.println("count: " + count);
		System.out.println("total: " + total);
		double percentage = (double)((double)count / (double)total) * 100;
		
		//for(SecondMapperValue value : valuesList) {
			context.write(
				new ThirdReducerKey(key.getIdHouse(), key.getHour()),
				new ThirdReducerValue(percentage));
		//}
	}
}
