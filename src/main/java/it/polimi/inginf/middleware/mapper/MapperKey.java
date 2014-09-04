package it.polimi.inginf.middleware.mapper;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;

public class MapperKey implements WritableComparable<MapperKey>{
	private IntWritable idHouse;
	private LongWritable hour;
	
	public MapperKey() {}
	
	public MapperKey(int idHouse, long hour) {
		this.idHouse = new IntWritable(idHouse);
		this.hour = new LongWritable(hour);
	}
	
	public void readFields(DataInput in) throws IOException {
		this.idHouse.readFields(in);
		this.hour.readFields(in);
	}

	public void write(DataOutput out) throws IOException {
		this.idHouse.write(out);
		this.hour.write(out);
	}

	public IntWritable getIdHouse() {
		return this.idHouse;
	}
	
	public LongWritable getHour() {
		return this.hour;
	}
	
	public int compareTo(MapperKey o) {
		if (o == null) {
			throw new NullPointerException();
		} else if (o == this) {
			return 0;
		}
		if (this.idHouse.compareTo(o.getIdHouse()) == 0) {
			return this.hour.compareTo(o.getHour());
		} else
			return this.idHouse.compareTo(o.getIdHouse());
	}
	
	public String toString() {
		return "House: " + this.idHouse.toString() +
				"Hour: " + this.hour.toString();
	}
}
