package it.polimi.inginf.middleware.secondJob.reducer;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class SecondReducerValue implements WritableComparable<SecondReducerValue> {
	private int medianPlug;
	private int medianHouse;
	
	public SecondReducerValue() {}
	
	public SecondReducerValue(int medianPlug, int medianHouse) {
		this.medianPlug = medianPlug;//new IntWritable(measure);
		this.medianHouse = medianHouse;
	}
	public void readFields(DataInput in) throws IOException {
		this.medianPlug = in.readInt();//.readFields(in);
		this.medianHouse = in.readInt();
	}

	public void write(DataOutput out) throws IOException {
		out.writeInt(this.medianPlug);//.write(out);
		out.writeInt(this.medianHouse);
	}
	
	public int getMedianPlug() {
		return this.medianPlug;
	}
	
	public int getMedianHouse() {
		return this.medianHouse;
	}
	
	public int compareTo(SecondReducerValue o) {
		if(this.medianPlug < o.getMedianPlug()) {
			return -1;
		}
		if(this.medianPlug > o.getMedianPlug()) {
			return 1;
		}
		if(this.medianHouse < o.getMedianHouse()) {
			return -1;
		}
		if(this.medianHouse > o.getMedianHouse()) {
			return 1;
		}
		return 0;
	}
	
	public String toString() {
		return this.medianPlug + "," +
				this.medianHouse;
	}
}
