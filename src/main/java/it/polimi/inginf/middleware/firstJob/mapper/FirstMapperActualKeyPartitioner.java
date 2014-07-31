package it.polimi.inginf.middleware.firstJob.mapper;

import org.apache.hadoop.mapred.lib.HashPartitioner;
import org.apache.hadoop.mapreduce.Partitioner;

public class FirstMapperActualKeyPartitioner extends Partitioner<FirstMapperCompositeKey, FirstMapperValue>{
	HashPartitioner<FirstMapperActualKey, FirstMapperValue> hashPartitioner = new HashPartitioner<FirstMapperActualKey, FirstMapperValue>();
	FirstMapperActualKey newKey;
	
	@Override
	public int getPartition(FirstMapperCompositeKey key, FirstMapperValue value, int numReduceTasks) {
		try {
			newKey = new FirstMapperActualKey(key.getIdHouse(), key.getHour());
			return hashPartitioner.getPartition(newKey, value, numReduceTasks);
		} catch (Exception e) {
			return (int) (Math.random() * numReduceTasks);
		}
	}
}