package cs1.wc;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

public class PairIntWritable implements WritableComparable<PairIntWritable> {
	private IntWritable left, right;
	
	
	
	public PairIntWritable() {
		super();
		left = new IntWritable();
		right = new IntWritable();
	}



	public PairIntWritable(IntWritable left, IntWritable right) {
		super();
		this.left = left;
		this.right = right;
	}

	public PairIntWritable(int left, int right) {
		super();
		this.left = new IntWritable(left);
		this.right = new IntWritable(right);
	}

	public IntWritable getLeft() {
		return left;
	}



	public void setLeft(IntWritable left) {
		this.left = left;
	}



	public IntWritable getRight() {
		return right;
	}



	public void setRight(IntWritable right) {
		this.right = right;
	}



	public void write(DataOutput out) throws IOException {
		left.write(out);
		right.write(out);
		
	}

	public void readFields(DataInput in) throws IOException {
		left.readFields(in);
		right.readFields(in);
	}

	public int compareTo(PairIntWritable o) {
		int cmp = left.compareTo(o.getLeft());
		if (cmp != 0) return cmp;
		else return right.compareTo(o.getRight());
		
	}

	@Override
	public int hashCode() {
		return left.hashCode()*163 + right.hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof PairIntWritable) {
			PairIntWritable other = (PairIntWritable)obj;
			return left.equals(other.getLeft()) && right.equals(other.getRight());
		}
		return false;
	}
	

}
