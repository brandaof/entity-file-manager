package org.brandao.entityfilemanager.tx;

public class TransactionFileNameMetadata {
	
	private String name;
	
	private long transactionID;

	public TransactionFileNameMetadata(String name, long transactionID) {
		super();
		this.name = name;
		this.transactionID = transactionID;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public long getTransactionID() {
		return transactionID;
	}

	public void setTransactionID(long transactionID) {
		this.transactionID = transactionID;
	}

}
