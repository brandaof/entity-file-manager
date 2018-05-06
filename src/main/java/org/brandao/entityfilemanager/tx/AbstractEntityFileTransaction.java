package org.brandao.entityfilemanager.tx;

import java.util.Map;

import org.brandao.entityfilemanager.EntityFileAccess;
import org.brandao.entityfilemanager.LockProvider;
import org.brandao.entityfilemanager.PersistenceException;
import org.brandao.entityfilemanager.tx.readcommited.PointerManager;
import org.brandao.entityfilemanager.tx.readcommited.TransactionalEntityFile;

public abstract class AbstractEntityFileTransaction 
	implements EntityFileTransaction{

	protected EntityFileTransactionManager entityFileTransactionManager;
	
	protected Map<EntityFileAccess<?,?>, TransactionalEntityFile<?,?>> transactionFiles;
	
	protected LockProvider lockProvider;
	
	protected long transactionID;
	
	protected boolean started;

	protected boolean rolledBack;
	
	protected boolean commited;
	
	protected boolean dirty;
	
	protected byte status;
	
	protected boolean closed;
	
	protected long timeout;
	
	public AbstractEntityFileTransaction(
			EntityFileTransactionManager entityFileTransactionManager,
			LockProvider lockProvider,
			Map<EntityFileAccess<?,?>, TransactionalEntityFile<?,?>> transactionFiles,
			byte status, long transactionID, boolean started, boolean rolledBack, 
			boolean commited, long timeout) {
		this.entityFileTransactionManager = entityFileTransactionManager;
		this.transactionFiles             = transactionFiles;
		this.transactionID                = transactionID;
		this.lockProvider                 = lockProvider;
		this.rolledBack                   = rolledBack;
		this.commited                     = commited;
		this.started                      = started;
		this.status                       = status;
		this.dirty                        = false;
	}

	public void setTimeout(long value) {
		this.timeout = value;
	}

	public long getTimeout() {
		return this.timeout;
	}
	
	public byte getStatus() {
		return this.status;
	}
	
	public boolean isClosed() {
		return this.closed;
	}
	
	public boolean isRolledBack() {
		return this.rolledBack;
	}

	public boolean isCommited() {
		return this.commited;
	}

	public void rollback() throws TransactionException {
		
		if(this.closed){
			throw new TransactionException("transaction has been closed");
		}
		
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
				txFile.setTransactionStatus(EntityFileTransaction.TRANSACTION_STARTED_ROLLBACK);
			}
			
			this.status = EntityFileTransaction.TRANSACTION_STARTED_ROLLBACK;
			
			for(TransactionalEntityFile<?,?> txFile: this.transactionFiles.values()){
				txFile.rollback();
			}
			
			for(TransactionalEntityFile<?,?> txFile: this.transactionFiles.values()){
				txFile.setTransactionStatus(EntityFileTransaction.TRANSACTION_ROLLEDBACK);
			}
			
			this.status = EntityFileTransaction.TRANSACTION_ROLLEDBACK;
			this.commited   = false;
			this.rolledBack = true;
		}
		catch(Throwable e){
			throw new TransactionException(e);
		}
	}

	public void commit() throws TransactionException {
		
		if(this.dirty){
			throw new TransactionException("transaction rollback is needed");
		}
		
		if(this.rolledBack){
			throw new TransactionException("transaction has been rolled back");
		}
		
		if(this.commited){
			throw new TransactionException("transaction has been commited");
		}

		if(this.closed){
			throw new TransactionException("transaction has been closed");
		}
		
		if(!started){
			throw new TransactionException("transaction not started");
		}
		
		try{
			for(TransactionalEntityFile<?,?> txFile: this.transactionFiles.values()){
				txFile.setTransactionStatus(EntityFileTransaction.TRANSACTION_STARTED_COMMIT);
			}

			this.status = EntityFileTransaction.TRANSACTION_STARTED_COMMIT;
			
			for(TransactionalEntityFile<?,?> txFile: this.transactionFiles.values()){
				txFile.rollback();
			}
			
			for(TransactionalEntityFile<?,?> txFile: this.transactionFiles.values()){
				txFile.setTransactionStatus(EntityFileTransaction.TRANSACTION_COMMITED);
			}
			
			this.status     = EntityFileTransaction.TRANSACTION_COMMITED;
			this.commited   = true;
			this.rolledBack = false;
			
		}
		catch(Throwable e){
			throw new TransactionException(e);
		}
		
	}

	public void begin() throws TransactionException {
		
		if(this.started){
			throw new TransactionException("transaction has been started");
		}

		this.started = true;
	}
	
	public void close() throws TransactionException{
		entityFileTransactionManager.closeTransaction(this);
	}
	
	/* private methods */
	
	@SuppressWarnings("unchecked")
	protected <T,R> TransactionalEntityFile<T,R> getManagedEntityFile( 
			EntityFileAccess<T,R> entityFile) throws PersistenceException{
		try{
			
			TransactionalEntityFile<T,R> tx = 
				(TransactionalEntityFile<T, R>) this.transactionFiles.get(entityFile);
			
			if(tx != null){
				return tx;
			}
			
			TransactionEntityFileAccess<T,R> txFile = 
				new TransactionEntityFileAccess<T,R>(entityFile, this.transactionID);
			txFile.createNewFile();
			
			tx = new TransactionalEntityFile<T, R>(
					entityFile, 
					txFile, 
					new PointerManager<T, R>(txFile, entityFile, lockProvider, this.timeout)
			);
			
			this.transactionFiles.put(entityFile, tx);
			return tx;
		}
		catch(Throwable e){
			throw new PersistenceException(e);
		}
	}

	/* restrict methods */
	
	public Map<EntityFileAccess<?,?>, TransactionalEntityFile<?,?>> getTransactionalEntityFile(){
		return this.transactionFiles;
	}
	
	public long getTransactionID(){
		return this.transactionID;
	}
	
	public void setClosed(boolean value){
		this.closed = value;
	}
	
	public boolean isStarted(){
		return this.started;
	}
	
	public boolean isDirty(){
		return this.dirty;
	}
	
	protected void finalize() throws Throwable{
		try{
			this.close();
		}
		finally{
			super.finalize();
		}
	}

}
