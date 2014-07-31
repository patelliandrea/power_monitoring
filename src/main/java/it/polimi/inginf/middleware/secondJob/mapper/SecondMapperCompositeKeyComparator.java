package it.polimi.inginf.middleware.secondJob.mapper;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class SecondMapperCompositeKeyComparator extends WritableComparator {
	protected SecondMapperCompositeKeyComparator() {
		super(SecondMapperCompositeKey.class, true);
	}
	
	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {
		SecondMapperCompositeKey key1 = (SecondMapperCompositeKey) w1;
		SecondMapperCompositeKey key2 = (SecondMapperCompositeKey) w2;
		
		if(key1.getIdHouse() < key2.getIdHouse()) {
			return -1;
		}
		if(key1.getIdHouse() > key2.getIdHouse()) {
			return 1;
		}
		if(key1.getIdHousehold() < key2.getIdHousehold()) {
			return -1;
		}
		if(key1.getIdHousehold() > key2.getIdHousehold()) {
			return 1;
		}
		if(key1.getIdPlug() < key2.getIdPlug()) {
			return -1;
		}
		if(key1.getIdPlug() > key2.getIdPlug()) {
			return 1;
		}
		if(key1.getHour() < key2.getHour()) {
			return -1;
		}
		if(key1.getHour() > key2.getHour()) {
			return 1;
		}
		if(key1.getMeasure() < key2.getMeasure()) {
			return -1;
		}
		if(key1.getMeasure() > key2.getMeasure()) {
			return 1;
		}
		return 0;
	}
}
