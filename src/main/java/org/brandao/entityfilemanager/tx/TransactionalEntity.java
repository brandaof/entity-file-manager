package org.brandao.entityfilemanager.tx;

public class TransactionalEntity<T> {

	private static final byte RESERVED_ROW = Byte.valueOf("00000001", 2);
	
	private long original;
	
	private byte flags;
	
	private T entity;
	
	public boolean isFree(){
		return (this.flags & RESERVED_ROW) == 0;
	}
	
	public T getEntity() {
		return entity;
	}

	public long getOriginal() {
		return original;
	}

}
