package org.brandao.entityfilemanager.tx;

public class TransactionHeader<H> {

	private H parent;

	private byte transactionStatus;
	
	private long transactionID;
	
	private byte transactionIsolation;
	
	public TransactionHeader(H parent) {
		this.parent = parent;
	}

	public H getParent() {
		return parent;
	}

	public void setParent(H parent) {
		this.parent = parent;
	}

	public byte getTransactionStatus() {
		return transactionStatus;
	}

	public void setTransactionStatus(byte transactionStatus) {
		this.transactionStatus = transactionStatus;
	}

	public long getTransactionID() {
		return transactionID;
	}

	public void setTransactionID(long transactionID) {
		this.transactionID = transactionID;
	}

	public byte getTransactionIsolation() {
		return transactionIsolation;
	}

	public void setTransactionIsolation(byte transactionIsolation) {
		this.transactionIsolation = transactionIsolation;
	}
	
}
