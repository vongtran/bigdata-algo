package cs1.cb;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.StringWriter;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class PairTextWritable implements WritableComparable<PairTextWritable> {
	private Text left, right;
	
	
	public PairTextWritable() {
		super();
		left = new Text();
		right = new Text();
	}


	public PairTextWritable(Text left, Text right) {
		super();
		this.left = left;
		this.right = right;
	}

	public PairTextWritable(String left, String right) {
		this.left = new Text(left);
		this.right = new Text(right);
	}

	public Text getLeft() {
		return left;
	}


	public void setLeft(Text left) {
		this.left = left;
	}


	public Text getRight() {
		return right;
	}


	public void setRight(Text right) {
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

	public int compareTo(PairTextWritable o) {
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
		if (obj instanceof PairTextWritable) {
			PairTextWritable other = (PairTextWritable)obj;
			return left.equals(other.getLeft()) && right.equals(other.getRight());
		}
		return false;
	}


	@Override
	public String toString() {
		
		return "<" + left.toString() + "," + right.toString() + ">";
	}
	

}
