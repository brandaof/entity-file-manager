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

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((entity == null) ? 0 : entity.hashCode());
		result = prime * result + flags;
		result = prime * result + (int) (recordID ^ (recordID >>> 32));
		return result;
	}

	@SuppressWarnings("rawtypes")
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		TransactionalEntity other = (TransactionalEntity) obj;
		if (entity == null) {
			if (other.entity != null)
				return false;
		} else if (!entity.equals(other.entity))
			return false;
		if (flags != other.flags)
			return false;
		if (recordID != other.recordID)
			return false;
		return true;
	}

}
