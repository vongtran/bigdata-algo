package cs1.wc;

public class Pair<U,V> {
	private U left;
	private V right;
	
	public Pair(U left, V right) {
		super();
		this.left = left;
		this.right = right;
	}

	public U getLeft() {
		return left;
	}
	public void setLeft(U left) {
		this.left = left;
	}
	public V getRight() {
		return right;
	}
	public void setRight(V right) {
		this.right = right;
	}
	
}
