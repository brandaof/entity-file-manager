package org.brandao.entityfilemanager.tx;

public class RawTransactionEntity<R> implements Comparable<RawTransactionEntity<R>>{

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

	public int compare(RawTransactionEntity<R> o1, RawTransactionEntity<R> o2) {
		return o1.recordID >= o2.recordID? 1 : -1;
	}

	public int compareTo(RawTransactionEntity<R> o) {
		return this.recordID >= o.recordID? 1 : -1;
	}
	
}
