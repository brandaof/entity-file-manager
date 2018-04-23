package org.brandao.entityfilemanager.tx;

import java.util.Map;

import org.brandao.entityfilemanager.EntityFileAccess;
import org.brandao.entityfilemanager.EntityFileManagerConfigurer;
import org.brandao.entityfilemanager.PersistenceException;

public class EntityFileTransactionHandlerImp 
	implements EntityFileTransactionHandler{

	private static final long TIME_OUT = 5*60*1000;
	
	protected EntityFileTransactionManager entityFileTransactionManager;
	
	protected Map<EntityFileAccess<?,?>, TransactionalEntityFile<?,?>> transactionFiles;
	
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
		
		if(this.rolledBack){
			throw new TransactionException("transaction has been rolled back");
		}

		if(this.commited){
			throw new TransactionException("transaction has been commited");
		}
		
		if(!started){
			throw new TransactionException("transaction not started");
		}
		
		try{
			for(TransactionalEntityFile<?,?> txFile: this.transactionFiles.values()){
				txFile.setTransactionStatus(TransactionEntityFileAccess.TRANSACTION_STARTED_ROLLBACK);
			}

			for(TransactionalEntityFile<?,?> txFile: this.transactionFiles.values()){
				txFile.rollback();
			}
			
			for(TransactionalEntityFile<?,?> txFile: this.transactionFiles.values()){
				txFile.setTransactionStatus(TransactionEntityFileAccess.TRANSACTION_ROLLBACK);
			}
		}
		catch(Throwable e){
			throw new TransactionException(e);
		}
	}

	public void commit() throws TransactionException {
		
		if(this.rolledBack)
			throw new TransactionException("transaction has been rolled back");

		if(this.commited)
			throw new TransactionException("transaction has been commited");
		
		if(!started)
			throw new TransactionException("transaction not started");

		try{
			for(TransactionalEntityFile<?,?> txFile: this.transactionFiles.values()){
				txFile.setTransactionStatus(TransactionEntityFileAccess.TRANSACTION_STARTED_COMMIT);
			}

			for(TransactionalEntityFile<?,?> txFile: this.transactionFiles.values()){
				txFile.rollback();
			}
			
			for(TransactionalEntityFile<?,?> txFile: this.transactionFiles.values()){
				txFile.setTransactionStatus(TransactionEntityFileAccess.TRANSACTION_COMMITED);
			}
		}
		catch(Throwable e){
			throw new TransactionException(e);
		}
		
	}

	public void begin() throws TransactionException {
		if(this.started)
			throw new TransactionException("transaction has been started");

		if(this.commitInProgress)
			throw new TransactionException("commit in progress");
		
		this.started = true;
	}

	public <T,R> long insert(T entity, EntityFileAccess<T,R> entityFileaccess)
			throws PersistenceException {
		TransactionalEntityFile<T,R> txEntityFileAccess = this.getManagedEntityFile(entityFileaccess);
		return txEntityFileAccess.insert(entity);
	}

	public <T,R> void update(long id, T entity,
			EntityFileAccess<T,R> entityFileaccess) throws PersistenceException {
		TransactionalEntityFile<T,R> txEntityFileAccess = this.getManagedEntityFile(entityFileaccess);
		txEntityFileAccess.update(id, entity);
	}

	public <T,R> void delete(long id, EntityFileAccess<T,R> entityFileaccess)
			throws PersistenceException {
		TransactionalEntityFile<T,R> txEntityFileAccess = this.getManagedEntityFile(entityFileaccess);
		txEntityFileAccess.delete(id);
	}

	public <T,R> T select(long id, EntityFileAccess<T,R> entityFileaccess) {
		TransactionalEntityFile<T,R> txEntityFileAccess = this.getManagedEntityFile(entityFileaccess);
		return txEntityFileAccess.select(id);
	}

	/* private methods */
	
	@SuppressWarnings("unchecked")
	private <T,R> TransactionalEntityFile<T,R> getManagedEntityFile( 
			EntityFileAccess<T,R> entityFile) throws PersistenceException{
		try{
			
			TransactionalEntityFile<T,R> tx = 
					(TransactionalEntityFile<T,R>)this.transactionFiles.get(entityFile);
			
			if(tx != null){
				return tx;
			}
			
			TransactionEntityFileAccess<T,R> txFile = 
				new TransactionEntityFileAccess<T,R>(entityFile, this.transactionID);
			txFile.createNewFile();
			this.transactionFiles.put(entityFile, tx);
			return tx;
		}
		catch(Throwable e){
			throw new PersistenceException(e);
		}
	}
	
}
