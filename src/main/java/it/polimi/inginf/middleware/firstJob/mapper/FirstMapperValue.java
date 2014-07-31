package it.polimi.inginf.middleware.firstJob.mapper;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class FirstMapperValue implements WritableComparable<FirstMapperValue> {
	private int id;
	private int idPlug;
	private int idHousehold;
	private int measure;
	
	public FirstMapperValue() {}
	
	public FirstMapperValue(FirstMapperValue other) {
		this();
		this.id = other.getId();
		this.idPlug = other.getIdPlug();
		this.idHousehold = other.getIdHousehold();
		this.measure = other.getMeasure();
	}
	
	public FirstMapperValue(int id, int idPlug, int idHousehold, int measure) {
		this.id = id;
		this.idPlug = idPlug;//new IntWritable(idPlug);
		this.idHousehold = idHousehold;//new IntWritable(idHousehold);
		this.measure = measure;//new IntWritable(measure);
	}
	public void readFields(DataInput in) throws IOException {
		this.id = in.readInt();
		this.idPlug = in.readInt();//.readFields(in);
		this.idHousehold = in.readInt();//.readFields(in);
		this.measure = in.readInt();//.readFields(in);
	}

	public void write(DataOutput out) throws IOException {
		out.writeInt(this.id);
		out.writeInt(this.idPlug);//.write(out);
		out.writeInt(this.idHousehold);//.write(out);
		out.writeInt(this.measure);//.write(out);
	}
	
	public int getId() {
		return this.id;
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
	
	public int compareTo(FirstMapperValue o) {
		if(this.measure < o.getMeasure()) {
			return -1;
		}
		if(this.measure > o.getMeasure()) {
			return 1;
		}
		return 0;
	}
}
