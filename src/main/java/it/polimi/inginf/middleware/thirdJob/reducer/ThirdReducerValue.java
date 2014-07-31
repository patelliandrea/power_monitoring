package it.polimi.inginf.middleware.thirdJob.reducer;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class ThirdReducerValue implements WritableComparable<ThirdReducerValue> {
	private double percentage;
	
	public ThirdReducerValue() {}
	
	public ThirdReducerValue(ThirdReducerValue other) {
		this();
		this.percentage = other.getPercentage();
	}
	
	public ThirdReducerValue(double percentage) {
		this.percentage = percentage;
	}
	public void readFields(DataInput in) throws IOException {
		this.percentage = in.readDouble();
	}

	public void write(DataOutput out) throws IOException {
		out.writeDouble(this.percentage);
	}
	
	public double getPercentage() {
		return this.percentage;
	}
	
	public int compareTo(ThirdReducerValue o) {
		if(this.percentage < o.getPercentage()) {
			return -1;
		}
		if(this.percentage > o.getPercentage()) {
			return 1;
		}
		return 0;
	}
	
	public String toString() {
		return Double.toString(this.percentage);
	}
}
