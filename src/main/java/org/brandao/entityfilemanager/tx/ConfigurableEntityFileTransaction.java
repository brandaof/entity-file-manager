package org.brandao.entityfilemanager.tx;

public interface ConfigurableEntityFileTransaction 
	extends EntityFileTransaction{

	void setStatus(byte value);
	
	void setRolledBack(boolean value);
	
	void setCommited(boolean value);
	
	void setClosed(boolean value);
	
	void setStarted(boolean value);
	
	boolean isStarted();

	long getTransactionID();
	
}
