package it.polimi.inginf.middleware.secondJob.mapper;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class SecondMapperValue implements WritableComparable<SecondMapperValue> {
	private int id;
	private int measure;
	//private int medianHouse;

	public SecondMapperValue() {}
	
	public SecondMapperValue(SecondMapperValue other) {
		this();
		this.id = other.getId();
		this.measure = other.getMeasure();
		//this.medianHouse = other.getMedianHouse();
	}
	
	public SecondMapperValue(int id, int measure) {
		this.id = id;
		this.measure = measure;
		//this.medianHouse = medianHouse;
	}
	
	public void readFields(DataInput in) throws IOException {
		this.id = in.readInt();
		this.measure = in.readInt();
		//this.medianHouse = in.readInt();
	}
	
	public void write(DataOutput out) throws IOException {
		out.writeInt(this.id);
		out.writeInt(this.measure);
		//out.writeInt(this.medianHouse);
	}
	
	public int getId() {
		return this.id;
	}
	
	public int getMeasure() {
		return this.measure;
	}
	
//	public int getMedianHouse() {
//		return this.medianHouse;
//	}
	
	public int compareTo(SecondMapperValue o) {
		if(this.measure < o.getMeasure()) {
			return -1;
		}
		if(this.measure > o.getMeasure()) {
			return 1;
		}
		return 0;
	}
}
