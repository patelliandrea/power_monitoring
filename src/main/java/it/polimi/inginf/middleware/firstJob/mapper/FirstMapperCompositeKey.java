package it.polimi.inginf.middleware.firstJob.mapper;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class FirstMapperCompositeKey implements WritableComparable<FirstMapperCompositeKey>{
	private int idHouse;
	private int hour;
	private int measure;
	
	public FirstMapperCompositeKey() {}
	
	public FirstMapperCompositeKey(int idHouse, int hour, int measure) {
		this.idHouse = idHouse;//new IntWritable(idHouse);
		this.hour = hour;//new IntWritable(hour);
		this.measure = measure;
	}
	public void readFields(DataInput in) throws IOException {
		this.idHouse = in.readInt();//.readFields(in);
		this.hour = in.readInt();//.readFields(in);
		this.measure = in.readInt();
	}

	public void write(DataOutput out) throws IOException {
		out.writeInt(idHouse);//this.idHouse.write(out);
		out.writeInt(hour);//this.hour.write(out);
		out.writeInt(measure);
	}

	public int getIdHouse() {
		return this.idHouse;
	}
	
	public int getHour() {
		return this.hour;
	}
	
	public int getMeasure() {
		return this.measure;
	}
	
	public int compareTo(FirstMapperCompositeKey o) {
		if(this.idHouse < o.getIdHouse()) {
			return -1;
		}
		if(this.idHouse > o.getIdHouse()) {
			return 1;
		}
		if(this.hour < o.getHour()) {
			return -1;
		}
		if(this.hour > o.getHour()) {
			return 1;
		}
		if(this.measure < o.getMeasure()) {
			return -1;
		}
		if(this.measure > o.getMeasure()) {
			return 1;
		}
		
		return 0;
	}
}
