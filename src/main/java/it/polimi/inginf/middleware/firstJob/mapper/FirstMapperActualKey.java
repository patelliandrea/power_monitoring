package it.polimi.inginf.middleware.firstJob.mapper;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class FirstMapperActualKey implements WritableComparable<FirstMapperActualKey>{
	private int idHouse;
	private int hour;
	
	public FirstMapperActualKey() {}
	
	public FirstMapperActualKey(int idHouse, int hour) {
		this.idHouse = idHouse;//new IntWritable(idHouse);
		this.hour = hour;//new IntWritable(hour);
	}
	public void readFields(DataInput in) throws IOException {
		this.idHouse = in.readInt();//.readFields(in);
		this.hour = in.readInt();//.readFields(in);
	}

	public void write(DataOutput out) throws IOException {
		out.writeInt(idHouse);//this.idHouse.write(out);
		out.writeInt(hour);//this.hour.write(out);
	}

	public int getIdHouse() {
		return this.idHouse;
	}
	
	public int getHour() {
		return this.hour;
	}
	
	public int compareTo(FirstMapperActualKey o) {
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
		
		return 0;
	}
}
