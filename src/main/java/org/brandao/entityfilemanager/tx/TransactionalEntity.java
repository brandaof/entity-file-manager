package org.brandao.entityfilemanager.tx;

public class TransactionalEntity<T> {

	public static final byte UPDATE_RECORD	= Byte.valueOf("00000001", 2);
	
	public static final byte NEW_RECORD		= Byte.valueOf("00000010", 2);
	
	public static final byte DELETE_RECORD	= Byte.valueOf("00000100", 2);
	
	private long recordID;
	
	private byte flags;
	
	private T entity;

	public TransactionalEntity(long recordID, byte flags, T entity) {
		this.recordID          = recordID;
		this.flags             = flags;
		this.entity            = entity;
	}

	public boolean isNewRecord(){
		return (this.flags & NEW_RECORD) == 0;
	}
	
	public boolean isUpdateRecord(){
		return (this.flags & NEW_RECORD) == 0;
	}
	
	public T getEntity() {
		return entity;
	}

	public long getRecordID() {
		return recordID;
	}

	public byte getFlags() {
		return flags;
	}

}
