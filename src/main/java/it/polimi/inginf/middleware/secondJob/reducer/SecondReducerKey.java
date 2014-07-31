package it.polimi.inginf.middleware.secondJob.reducer;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class SecondReducerKey implements WritableComparable<SecondReducerKey> {
	private int idHouse;
	private int hour;
	private int idHousehold;
	private int idPlug;
	
	public SecondReducerKey() {}
	
	public SecondReducerKey(int idHouse, int hour, int idHousehold, int idPlug) {
		this.idHouse = idHouse;
		this.hour = hour;
		this.idHousehold = idHousehold;
		this.idPlug = idPlug;
	}
	
	public void readFields(DataInput in) throws IOException {
		this.idHouse = in.readInt();
		this.hour = in.readInt();
		this.idHousehold = in.readInt();
		this.idPlug = in.readInt();
	}
	
	public void write(DataOutput out) throws IOException {
		out.writeInt(this.idHouse);
		out.writeInt(this.hour);
		out.writeInt(this.idHousehold);
		out.writeInt(this.idPlug);
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
	
	public int compareTo(SecondReducerKey o) {
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
	
	public String toString() {
		return this.idHouse + "," +
				this.idHousehold + "," +
				this.idPlug + "," +
				this.hour + ",";
	}
}
