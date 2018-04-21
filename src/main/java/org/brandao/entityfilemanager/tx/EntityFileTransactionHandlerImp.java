package org.brandao.entityfilemanager.tx;

import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;

import org.brandao.entityfilemanager.EntityFileAccess;
import org.brandao.entityfilemanager.EntityFileManagerConfigurer;
import org.brandao.entityfilemanager.PersistenceException;

public class EntityFileTransactionHandlerImp 
	implements EntityFileTransactionHandler{

	private static final long TIME_OUT = 5*60*1000;
	
	protected EntityFileTransactionManager entityFileTransactionManager;
	
	protected Map<EntityFileAccess<?>, EntityFileAccessTransaction<?>> transactionFiles;
	
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

	public <T> long insert(T entity, EntityFileAccess<T> entityFileaccess)
			throws PersistenceException {
		
		EntityFileAccess<Long> freePointerEntityFileaccess = null;
		EntityFileAccessTransaction<T> txEntityFileAccess  = this.getManagedEntityFile(entityFileaccess);
		
		ReadWriteLock readWritelock = entityFileaccess.getLock();
		
		Lock lock = readWritelock.writeLock();
		lock.lock();
		
		long id = -1;
		
		try{
			
			if(freePointerEntityFileaccess.length() == 0){
				id = entityFileaccess.length();
				
				txEntityFileAccess.seek(txEntityFileAccess.length());
				txEntityFileAccess.write(new TransactionalEntity<T>(id, TransactionalEntity.NEW_RECORD, entity));
				
				entityFileaccess.seek(entityFileaccess.length());
				entityFileaccess.write(null);
			}
			else{
				freePointerEntityFileaccess.seek(freePointerEntityFileaccess.length());
				id = freePointerEntityFileaccess.read();
				
				txEntityFileAccess.seek(txEntityFileAccess.length());
				txEntityFileAccess.write(new TransactionalEntity<T>(id, TransactionalEntity.NEW_RECORD, entity));
				
				freePointerEntityFileaccess.setLength(freePointerEntityFileaccess.length() - 1);
			}
			
			return id;
		}
		catch(Throwable e){
			throw new PersistenceException(e);
		}
		finally{
			lock.unlock();
		}
	}

	public <T> void update(long id, T entity,
			EntityFileAccess<T> entityFileaccess) throws PersistenceException {
	}

	public <T> void delete(long id, EntityFileAccess<T> entityFileaccess)
			throws PersistenceException {
	}

	public <T> T select(long id, EntityFileAccess<T> entityFileaccess) {
		return null;
	}

	/* private methods */
	
	@SuppressWarnings("unchecked")
	private <T> EntityFileAccessTransaction<T> getManagedEntityFile( 
			EntityFileAccess<T> entityFile) throws PersistenceException{
		try{
			
			EntityFileAccessTransaction<T> tx = 
					(EntityFileAccessTransaction<T>)this.transactionFiles.get(entityFile);
			
			if(tx != null){
				return tx;
			}
			
			tx = new EntityFileAccessTransaction<T>(entityFile, this.transactionID);
			tx.createNewFile();
			tx.setTransactionStatus(EntityFileAccessTransaction.TRANSACTION_NOT_STARTED);
			
			this.transactionFiles.put(entityFile, tx);
			return tx;
		}
		catch(Throwable e){
			throw new PersistenceException(e);
		}
	}
	
}
