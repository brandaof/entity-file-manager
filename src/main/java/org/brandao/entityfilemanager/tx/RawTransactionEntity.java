package org.brandao.entityfilemanager.tx;

public class RawTransactionEntity<R> {

	private long recordID;
	
	private byte flags;
	
	private R entity;

	public RawTransactionEntity(long recordID, byte flags, R entity) {
		this.recordID = recordID;
		this.flags = flags;
		this.entity = entity;
	}

	public long getRecordID() {
		return recordID;
	}

	public void setRecordID(long recordID) {
		this.recordID = recordID;
	}

	public byte getFlags() {
		return flags;
	}

	public void setFlags(byte flags) {
		this.flags = flags;
	}

	public R getEntity() {
		return entity;
	}

	public void setEntity(R entity) {
		this.entity = entity;
	}
	
}
