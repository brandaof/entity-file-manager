package org.brandao.entityfilemanager.tx;

import java.util.Map;

import org.brandao.entityfilemanager.EntityFile.Entity;
import org.brandao.entityfilemanager.EntityFileAccess;
import org.brandao.entityfilemanager.EntityFileManagerConfigurer;
import org.brandao.entityfilemanager.PersistenceException;

public class EntityFileTransactionHandlerImp 
	implements EntityFileTransactionHandler{

	private static final long TIME_OUT = 5*60*1000;
	
	protected EntityFileTransactionManager entityFileTransactionManager;
	
	protected Map<String, EntityFileAccessTransaction<?>> transactionFiles;
	
	private EntityFileManagerConfigurer manager;
	
	private long transactionID;
	
	private boolean commitInProgress;
	
	private boolean started;

	private boolean rolledBack;
	
	private boolean commited;
	
	public boolean isRolledBack() {
		return false;
	}

	public boolean isCommited() {
		return false;
	}

	public void rollback() throws TransactionException {
	}

	public void commit() throws TransactionException {
	}

	public void begin() throws TransactionException {
	}

	public <T> Entity<T> insert(T entity, EntityFileAccess<T> entityFileaccess)
			throws PersistenceException {
		return null;
	}

	public <T> Entity<T> update(long id, T entity,
			EntityFileAccess<T> entityFileaccess) throws PersistenceException {
		return null;
	}

	public <T> void delete(long id, EntityFileAccess<T> entityFileaccess)
			throws PersistenceException {
	}

	public <T> T select(long id, EntityFileAccess<T> entityFileaccess) {
		return null;
	}

	/* private methods */
	
	@SuppressWarnings("unchecked")
	private EntityFileAccessTransaction<?> getManagedEntityFile(String name, 
			EntityFileAccess<?> entityFile) throws PersistenceException{
		try{
			
			EntityFileAccessTransaction<?> tx = this.transactionFiles.get(name);
			
			if(tx != null){
				return tx;
			}
			
			tx = new EntityFileAccessTransaction(entityFile, this.transactionID);
			
			this.transactionFiles.put(name, tx);
			return tx;
		}
		catch(Throwable e){
			throw new PersistenceException(e);
		}
	}
	
}
