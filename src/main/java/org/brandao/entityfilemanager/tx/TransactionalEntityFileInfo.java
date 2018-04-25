package org.brandao.entityfilemanager.tx;

import java.util.HashSet;
import java.util.Set;

public class TransactionalEntityFileInfo<T,R> {

	public TransactionalEntityFile<T,R> entityFile;
	
	public Set<Long> managedRecords;
	
	public TransactionalEntityFileInfo(TransactionalEntityFile<T,R> entityFile){
		this.entityFile     = entityFile;
		this.managedRecords = new HashSet<Long>();
	}

	public TransactionalEntityFile<T, R> getEntityFile() {
		return entityFile;
	}

	public Set<Long> getManagedRecords() {
		return managedRecords;
	}
	
}
