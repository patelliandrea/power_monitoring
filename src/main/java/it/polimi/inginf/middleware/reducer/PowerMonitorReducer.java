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
		
		// all { idPlug, measure } of { idHouse, hour } to compute median value
		List<Long> valueList = new ArrayList<Long>();
		
		// map for mapping { idPlug }, { measure } in order to compute median of the plug in the hour
		Map<Integer, List<Long>> plugMeasures = new HashMap<Integer, List<Long>>();
		
		for(MapperValue value : values) {
			// add all measures in the valueList
			valueList.add(value.getMeasure().get());
			
			Integer idPlug = new Integer(value.getIdPlug().get());
			Long measure = value.getMeasure().get();
			
			if(plugMeasures.containsKey(idPlug)) {	// if idPlug is already key of the map
				plugMeasures.get(idPlug).add(measure);	// add the measure to the corresponding measure list
			} else {
				plugMeasures.put(idPlug, new ArrayList<Long>(Arrays.asList(measure))); // else put idPlug in the map and create the measure list
			}
		}
		
		// counter of the plugs with median value greater than the median of the house
		int greaterCount = 0;
		// compute the median of the house
		Long medianHouse = median(valueList);
		
		// for each plug in the map keySet
		for(Integer idPlug : plugMeasures.keySet()) {
			// compute median of the plug and check if it is greater than the median of the house
			if(median(plugMeasures.get(idPlug)) > medianHouse) {
				greaterCount++;
			}
		}
		
		// percentage of plugs with median greater of the median of the house
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