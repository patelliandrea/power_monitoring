package it.polimi.inginf.middleware.firstJob.mapper;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class FirstMapperActualKeyGroupingComparator extends WritableComparator {
	protected FirstMapperActualKeyGroupingComparator() {
		super(FirstMapperCompositeKey.class, true);
	}
	
	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {
		FirstMapperCompositeKey key1 = (FirstMapperCompositeKey) w1;
		FirstMapperCompositeKey key2 = (FirstMapperCompositeKey) w2;
		
		if(key1.getIdHouse() < key2.getIdHouse()) {
			return -1;
		}
		if(key1.getIdHouse() > key2.getIdHouse()) {
			return 1;
		}
		if(key1.getHour() < key2.getHour()) {
			return -1;
		}
		if(key1.getHour() > key2.getHour()) {
			return 1;
		}
		return 0;
	}
}