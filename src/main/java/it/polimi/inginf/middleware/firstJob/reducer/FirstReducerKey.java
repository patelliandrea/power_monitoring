package it.polimi.inginf.middleware.firstJob.reducer;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class FirstReducerKey implements WritableComparable<FirstReducerKey> {
	private int id;
	
	public FirstReducerKey() {}
	
	public FirstReducerKey(int id) {
		this.id = id;
	}
	
	public void readFields(DataInput in) throws IOException {
		this.id = in.readInt();
	}
	
	public void write(DataOutput out) throws IOException {
		out.writeInt(id);
	}
	
	public int getId() {
		return this.id;
	}
	
	public int compareTo(FirstReducerKey o) {
		if(this.id < o.getId()) {
			return -1;
		}
		if(this.id > o.getId()) {
			return 1;
		}
		return 0;
	}
	
	public String toString() {
		return Integer.toString(id) + ",";
	}
}
