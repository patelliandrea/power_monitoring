package it.polimi.inginf.middleware.firstJob.reducer;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class FirstReducerValue implements WritableComparable<FirstReducerValue> {
	private int idHouse;
	private int hour;
	private int idHousehold;
	private int idPlug;
	private int measure;
	private int medianHouse;
	
	public FirstReducerValue() {}
	
	public FirstReducerValue(int idHouse, int hour, int idPlug, int idHousehold, int measure, int medianHouse) {
		this.idHouse = idHouse;
		this.hour = hour;
		this.idPlug = idPlug;//new IntWritable(idPlug);
		this.idHousehold = idHousehold;//new IntWritable(idHousehold);
		this.measure = measure;//new IntWritable(measure);
		this.medianHouse = medianHouse;
	}
	public void readFields(DataInput in) throws IOException {
		this.idHouse = in.readInt();
		this.hour = in.readInt();
		this.idPlug = in.readInt();//.readFields(in);
		this.idHousehold = in.readInt();//.readFields(in);
		this.measure = in.readInt();//.readFields(in);
		this.medianHouse = in.readInt();
	}

	public void write(DataOutput out) throws IOException {
		out.writeInt(this.idHouse);
		out.writeInt(this.hour);
		out.writeInt(this.idPlug);//.write(out);
		out.writeInt(this.idHousehold);//.write(out);
		out.writeInt(this.measure);//.write(out);
		out.writeInt(this.medianHouse);
	}
	
	public int getMeasure() {
		return this.measure;
	}
	
	public int getIdPlug() {
		return this.idPlug;
	}
	
	public int getIdHousehold() {
		return this.idHousehold;
	}
	
	public int getIdHouse() {
		return this.idHouse;
	}
	
	public int getHour() {
		return this.hour;
	}
	
	public int getMedianHouse() {
		return this.medianHouse;
	}
	
	public int compareTo(FirstReducerValue o) {
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
	
	public String toString() {
		return this.idHouse + "," +
				this.hour + "," +
				this.medianHouse + "," +
				this.idHousehold + "," +
				this.idPlug + "," +
				this.measure;
	}
}
