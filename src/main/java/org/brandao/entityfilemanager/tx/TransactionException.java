package org.brandao.entityfilemanager.tx;

public class TransactionException 
	extends Exception{

	private static final long serialVersionUID = -6285171955924098328L;

	public TransactionException() {
		super();
	}

	public TransactionException(String message, Throwable cause) {
		super(message, cause);
	}

	public TransactionException(String message) {
		super(message);
	}

	public TransactionException(Throwable cause) {
		super(cause);
	}

}
