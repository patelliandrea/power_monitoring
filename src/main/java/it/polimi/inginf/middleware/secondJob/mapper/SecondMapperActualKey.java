package it.polimi.inginf.middleware.secondJob.mapper;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class SecondMapperActualKey implements WritableComparable<SecondMapperActualKey> {
	private int idHouse;
	private int hour;
	private int idHousehold;
	private int idPlug;
	private int medianHouse;
	
	public SecondMapperActualKey() {}
	
	public SecondMapperActualKey(int idHouse, int hour, int idHousehold, int idPlug, int medianHouse) {
		this.idHouse = idHouse;
		this.hour = hour;
		this.idHousehold = idHousehold;
		this.idPlug = idPlug;
		this.medianHouse = medianHouse;
	}
	
	public void readFields(DataInput in) throws IOException {
		this.idHouse = in.readInt();
		this.hour = in.readInt();
		this.idHousehold = in.readInt();
		this.idPlug = in.readInt();
		this.medianHouse = in.readInt();
	}
	
	public void write(DataOutput out) throws IOException {
		out.writeInt(this.idHouse);
		out.writeInt(this.hour);
		out.writeInt(this.idHousehold);
		out.writeInt(this.idPlug);
		out.writeInt(this.medianHouse);
	}
	
	public int getIdHouse() {
		return this.idHouse;
	}
	
	public int getHour() {
		return this.hour;
	}
	
	public int getIdHousehold() {
		return this.idHousehold;
	}
	
	public int getIdPlug() {
		return this.idPlug;
	}
	
	public int getMedianHouse() {
		return this.medianHouse;
	}
	
	public int compareTo(SecondMapperActualKey o) {
		if(this.idHouse < o.getIdHouse()) {
			return -1;
		}
		if(this.idHouse > o.getIdHouse()) {
			return 1;
		}
		if(this.idHousehold < o.getIdHousehold()) {
			return -1;
		}
		if(this.idHousehold > o.getIdHousehold()) {
			return 1;
		}
		if(this.idPlug < o.getIdPlug()) {
			return -1;
		}
		if(this.idPlug > o.getIdPlug()) {
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
