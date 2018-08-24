package org.brandao.entityfilemanager.tx;

import java.util.Map;

import org.brandao.entityfilemanager.EntityFileAccess;
import org.brandao.entityfilemanager.EntityFileException;
import org.brandao.entityfilemanager.LockProvider;

public abstract class AbstractEntityFileTransaction 
	implements EntityFileTransaction{

	protected EntityFileTransactionManagerConfigurer entityFileTransactionManager;
	
	protected Map<EntityFileAccess<?,?,?>, TransactionEntity<?,?>> transactionFiles;
	
	protected LockProvider lockProvider;
	
	protected long transactionID;
	
	protected boolean started;

	protected boolean rolledBack;
	
	protected boolean commited;
	
	protected boolean dirty;
	
	protected byte status;
	
	protected boolean closed;
	
	protected long timeout;
	
	protected byte transactionIsolation;
	
	public AbstractEntityFileTransaction(
			EntityFileTransactionManagerConfigurer entityFileTransactionManager,
			LockProvider lockProvider,
			Map<EntityFileAccess<?,?,?>, TransactionEntity<?,?>> transactionFiles,
			byte status, long transactionID, byte transactionIsolation, boolean started, boolean rolledBack, 
			boolean commited, long timeout) {
		this.entityFileTransactionManager = entityFileTransactionManager;
		this.transactionIsolation         = transactionIsolation;
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
			for(TransactionEntity<?,?> txFile: this.transactionFiles.values()){
				txFile.setTransactionStatus(EntityFileTransaction.TRANSACTION_STARTED_ROLLBACK);
			}
			
			this.status = EntityFileTransaction.TRANSACTION_STARTED_ROLLBACK;
			
			for(TransactionEntity<?,?> txFile: this.transactionFiles.values()){
				txFile.rollback();
			}
			
			for(TransactionEntity<?,?> txFile: this.transactionFiles.values()){
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
			for(TransactionEntity<?,?> txFile: this.transactionFiles.values()){
				txFile.setTransactionStatus(EntityFileTransaction.TRANSACTION_STARTED_COMMIT);
			}

			this.status = EntityFileTransaction.TRANSACTION_STARTED_COMMIT;
			
			for(TransactionEntity<?,?> txFile: this.transactionFiles.values()){
				txFile.commit();
			}
			
			for(TransactionEntity<?,?> txFile: this.transactionFiles.values()){
				txFile.setTransactionStatus(EntityFileTransaction.TRANSACTION_COMMITED);
			}

			this.status     = EntityFileTransaction.TRANSACTION_COMMITED;
			this.commited   = true;
			this.rolledBack = false;

			for(TransactionEntity<?,?> txFile: this.transactionFiles.values()){
				txFile.close();
			}

			for(TransactionEntity<?,?> txFile: this.transactionFiles.values()){
				txFile.delete();
			}
			
		}
		catch(Throwable e){
			throw new TransactionException(e);
		}
		
	}

	/*
	public void begin() throws TransactionException {
		
		if(this.started){
			throw new TransactionException("transaction has been started");
		}

		this.started = true;
	}
	*/
	
	public void close() throws TransactionException{
		entityFileTransactionManager.closeTransaction(this);
	}
	
	public <T, R, H> long insert(T entity, EntityFileAccess<T, R, H> entityFileAccess)
			throws EntityFileException {
		TransactionEntity<T,R> tei = this.getManagedEntityFile(entityFileAccess);
		return tei.insert(entity);
	}

	public <T, R, H> long[] insert(T[] entity,
			EntityFileAccess<T, R, H> entityFileAccess) throws EntityFileException {
		TransactionEntity<T,R> tei = this.getManagedEntityFile(entityFileAccess);
		return tei.insert(entity);
	}

	public <T, R, H> void update(long id, T entity,
			EntityFileAccess<T, R, H> entityFileAccess) throws EntityFileException {
		TransactionEntity<T,R> tei = this.getManagedEntityFile(entityFileAccess);
		tei.update(id, entity);
	}

	public <T, R, H> void update(long[] id, T[] entity,
			EntityFileAccess<T, R, H> entityFileAccess) throws EntityFileException {
		TransactionEntity<T,R> tei = this.getManagedEntityFile(entityFileAccess);
		tei.update(id, entity);
	}

	public <T, R, H> void delete(long id, EntityFileAccess<T, R, H> entityFileAccess)
			throws EntityFileException {
		TransactionEntity<T,R> tei = this.getManagedEntityFile(entityFileAccess);
		tei.delete(id);
	}

	public <T, R, H> void delete(long[] id, EntityFileAccess<T, R, H> entityFileAccess)
			throws EntityFileException {
		TransactionEntity<T,R> tei = this.getManagedEntityFile(entityFileAccess);
		tei.delete(id);
	}

	public <T, R, H> T select(long id, EntityFileAccess<T, R, H> entityFileAccess)
			throws EntityFileException {
		TransactionEntity<T,R> tei = this.getManagedEntityFile(entityFileAccess);
		return tei.select(id);
	}

	public <T, R, H> T[] select(long[] id, EntityFileAccess<T, R, H> entityFileAccess)
			throws EntityFileException {
		TransactionEntity<T,R> tei = this.getManagedEntityFile(entityFileAccess);
		return tei.select(id);
	}

	public <T, R, H> T select(long id, boolean lock,
			EntityFileAccess<T, R, H> entityFileAccess) throws EntityFileException {
		TransactionEntity<T,R> tei = this.getManagedEntityFile(entityFileAccess);
		return tei.select(id, lock);
	}

	public <T, R, H> T[] select(long[] id, boolean lock,
			EntityFileAccess<T, R, H> entityFileAccess) throws EntityFileException {
		TransactionEntity<T,R> tei = this.getManagedEntityFile(entityFileAccess);
		return tei.select(id, lock);
	}
	
	/* private methods */
	
	@SuppressWarnings("unchecked")
	protected <T,R,H> TransactionEntity<T,R> getManagedEntityFile( 
			EntityFileAccess<T,R,H> entityFile) throws EntityFileException{
		try{
			
			TransactionEntity<T,R> tx = 
				(TransactionEntity<T, R>) this.transactionFiles.get(entityFile);
			
			if(tx != null){
				return tx;
			}
			
			TransactionEntityFileAccess<T,R,H> txFile = 
					EntityFileTransactionUtil
						.getTransactionEntityFileAccess(
								entityFile, 
								transactionID, 
								transactionIsolation, this.entityFileTransactionManager);
			
			txFile.createNewFile();
			
			tx = this.createTransactionalEntityFile(entityFile, txFile);
			
			this.transactionFiles.put(entityFile, tx);
			return tx;
		}
		catch(Throwable e){
			throw new EntityFileException(e);
		}
	}

	protected abstract <T,R,H> TransactionEntity<T,R> createTransactionalEntityFile(
			EntityFileAccess<T,R,H> entityFile, TransactionEntityFileAccess<T,R,H> txFile);
	
	/* restrict methods */
	
	public Map<EntityFileAccess<?,?,?>, TransactionEntity<?,?>> getTransactionalEntityFile(){
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
