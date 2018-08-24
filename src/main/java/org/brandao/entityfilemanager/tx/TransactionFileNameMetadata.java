package org.brandao.entityfilemanager.tx;

import java.io.File;

public class TransactionFileNameMetadata {
	
	private String name;
	
	private long transactionID;

	private File file;
	
	public TransactionFileNameMetadata(String name, long transactionID, File file) {
		this.name = name;
		this.transactionID = transactionID;
		this.file = file;
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

	public File getFile() {
		return file;
	}

	public void setFile(File file) {
		this.file = file;
	}

}
