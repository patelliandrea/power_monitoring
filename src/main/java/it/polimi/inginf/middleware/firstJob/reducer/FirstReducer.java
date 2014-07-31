package it.polimi.inginf.middleware.firstJob.reducer;

import it.polimi.inginf.middleware.firstJob.mapper.FirstMapperCompositeKey;
import it.polimi.inginf.middleware.firstJob.mapper.FirstMapperValue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.mapreduce.Reducer;

public class FirstReducer extends Reducer<FirstMapperCompositeKey, FirstMapperValue, FirstReducerKey, FirstReducerValue> {
	@Override
	protected void reduce(FirstMapperCompositeKey key, Iterable<FirstMapperValue> values, Context context) throws IOException, InterruptedException {
		List<FirstMapperValue> valuesList = new ArrayList<FirstMapperValue>();
		//CollectionUtils.addAll(valuesList, values.iterator());
		for(FirstMapperValue value : values) {
			valuesList.add(new FirstMapperValue(value));
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
		
		for(FirstMapperValue value : valuesList) {
			context.write(
				new FirstReducerKey(value.getId()),
				new FirstReducerValue(key.getIdHouse(), key.getHour(), value.getIdPlug(), value.getIdHousehold(), value.getMeasure(), median)
			);
		}
	}
}