package it.polimi.inginf.middleware.secondJob.mapper;

import org.apache.hadoop.mapred.lib.HashPartitioner;
import org.apache.hadoop.mapreduce.Partitioner;

public class SecondMapperActualKeyPartitioner extends Partitioner<SecondMapperCompositeKey, SecondMapperValue>{
	HashPartitioner<SecondMapperActualKey, SecondMapperValue> hashPartitioner = new HashPartitioner<SecondMapperActualKey, SecondMapperValue>();
	SecondMapperActualKey newKey;
	
	@Override
	public int getPartition(SecondMapperCompositeKey key, SecondMapperValue value, int numReduceTasks) {
		try {
			newKey = new SecondMapperActualKey(key.getIdHouse(), key.getHour(), key.getIdHousehold(), key.getIdPlug(), key.getMedianHouse());
			return hashPartitioner.getPartition(newKey, value, numReduceTasks);
		} catch (Exception e) {
			return (int) (Math.random() * numReduceTasks);
		}
	}
}
