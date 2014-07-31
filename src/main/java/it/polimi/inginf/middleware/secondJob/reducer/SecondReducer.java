package it.polimi.inginf.middleware.secondJob.reducer;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.mapreduce.Reducer;

import it.polimi.inginf.middleware.secondJob.mapper.SecondMapperCompositeKey;
import it.polimi.inginf.middleware.secondJob.mapper.SecondMapperValue;
public class SecondReducer extends Reducer<SecondMapperCompositeKey, SecondMapperValue, SecondReducerKey, SecondReducerValue>{
	@Override
	protected void reduce(SecondMapperCompositeKey key, Iterable<SecondMapperValue> values, Context context) throws IOException, InterruptedException {
		List<SecondMapperValue> valuesList = new ArrayList<SecondMapperValue>();
		for(SecondMapperValue value : values) {
			valuesList.add(new SecondMapperValue(value));
		}
		int size = valuesList.size();
		int median = 0;
		
		if(size % 2 == 0) {
			int half = size / 2;
			median = valuesList.get(half).getMeasure();
		} else {
			int half = (size + 1) / 2;
			median = valuesList.get(half - 1).getMeasure();
		}
		
		//for(SecondMapperValue value : valuesList) {
			context.write(
				new SecondReducerKey(key.getIdHouse(), key.getHour(), key.getIdHousehold(), key.getIdPlug()),
				new SecondReducerValue(median, key.getMedianHouse()));
		//}
	}
}
