package cs1.cb;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;

public class ExtendMapWritable extends MapWritable{

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("[");
		for (Entry<Writable, Writable> e : entrySet()) {
			sb.append("{").append(e.getKey()).append("=").append(e.getValue()).append("}");
		}
		sb.append("]");
		return sb.toString();
	}
	
}
