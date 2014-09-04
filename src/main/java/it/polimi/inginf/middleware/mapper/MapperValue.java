package it.polimi.inginf.middleware.mapper;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;

public class MapperValue implements WritableComparable<MapperValue> {
	private IntWritable idPlug;
	private LongWritable measure;
	
	public MapperValue() {
		this.idPlug = new IntWritable();
		this.measure = new LongWritable();
	}
	
	public MapperValue(MapperValue other) {
		this();
		this.idPlug = other.getIdPlug();
		this.measure = other.getMeasure();
	}
	
	public MapperValue(int idPlug, int measure) {
		this.idPlug = new IntWritable(idPlug);
		this.measure = new LongWritable(measure);
	}
	
	public void readFields(DataInput in) throws IOException {
		this.idPlug.readFields(in);
		this.measure.readFields(in);
	}

	public void write(DataOutput out) throws IOException {
		this.idPlug.write(out);
		this.measure.write(out);
	}
	
	public LongWritable getMeasure() {
		return this.measure;
	}
	
	public IntWritable getIdPlug() {
		return this.idPlug;
	}
	
	public int compareTo(MapperValue o) {
		if(o == null) {
			throw new NullPointerException();
		}
		
		if(o == this) {
			return 0;
		}
		
		if (this.idPlug.compareTo(o.getIdPlug()) == 0) {
			return this.measure.compareTo(o.getMeasure());
		} else
			return this.idPlug.compareTo(o.idPlug);
	}
	
	public String toString() {
		return "Plug: " + this.idPlug.toString() +
				"Measure: " + this.measure.toString();
	}
}
