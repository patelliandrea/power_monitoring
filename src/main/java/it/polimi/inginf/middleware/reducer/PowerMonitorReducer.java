package it.polimi.inginf.middleware.reducer;

import it.polimi.inginf.middleware.mapper.MapperKey;
import it.polimi.inginf.middleware.mapper.MapperValue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class PowerMonitorReducer extends Reducer<MapperKey, MapperValue, MapperKey, FloatWritable> {
	@Override
	protected void reduce(MapperKey key, Iterable<MapperValue> values, Context context) throws IOException, InterruptedException {
		
		List<Long> valueList = new ArrayList<Long>();
		Map<Integer, List<Long>> plugMeasures = new HashMap<Integer, List<Long>>();
		
		for(MapperValue value : values) {
			valueList.add(value.getMeasure().get());
			
			Integer idPlug = new Integer(value.getIdPlug().get());
			Long measure = value.getMeasure().get();
			
			if(plugMeasures.containsKey(idPlug)) {
				plugMeasures.get(idPlug).add(measure);
			} else {
				plugMeasures.put(idPlug, new ArrayList<Long>(Arrays.asList(measure)));
			}
		}
		
		int greaterCount = 0;
		Long medianHouse = median(valueList);
		
		for(Integer idPlug : plugMeasures.keySet()) {
			if(median(plugMeasures.get(idPlug)) > medianHouse) {
				greaterCount++;
			}
		}
		
		int percentage = greaterCount * 100 / plugMeasures.keySet().size();
		
		context.write(key, new FloatWritable(percentage));
	}
	
	private Long median(List<Long> values) {
		int size = values.size();
		Collections.sort(values);
		if(size % 2 == 0) {
			return values.get(size / 2 - 1);
		} else {
			return values.get(size / 2);
		}
	}
}