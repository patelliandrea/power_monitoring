package it.polimi.inginf.middleware.thirdJob.mapper;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class ThirdMapperValue implements WritableComparable<ThirdMapperValue> {
	private int medianHouse;
	private int medianPlug;
	
	public ThirdMapperValue() {}
	
	public ThirdMapperValue(ThirdMapperValue other) {
		this();
		this.medianHouse = other.getMedianHouse();
		this.medianPlug = other.getMedianPlug();
	}
	
	public ThirdMapperValue(int medianHouse, int medianPlug) {
		this.medianHouse = medianHouse;
		this.medianPlug = medianPlug;
	}
	public void readFields(DataInput in) throws IOException {
		this.medianHouse = in.readInt();
		this.medianPlug = in.readInt();
	}

	public void write(DataOutput out) throws IOException {
		out.writeInt(this.medianHouse);
		out.writeInt(this.medianPlug);
	}
	
	public int getMedianHouse() {
		return this.medianHouse;
	}
	
	public int getMedianPlug() {
		return this.medianPlug;
	}
	
	public int compareTo(ThirdMapperValue o) {
		if(this.medianHouse < o.getMedianHouse()) {
			return -1;
		}
		if(this.medianHouse > o.getMedianHouse()) {
			return 1;
		}
		if(this.medianPlug < o.getMedianPlug()) {
			return -1;
		}
		if(this.medianPlug > o.getMedianPlug()) {
			return 1;
		}
		return 0;
	}
}
