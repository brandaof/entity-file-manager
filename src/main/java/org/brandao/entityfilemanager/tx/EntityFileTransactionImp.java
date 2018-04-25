package org.brandao.entityfilemanager.tx;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.brandao.entityfilemanager.EntityFileAccess;
import org.brandao.entityfilemanager.EntityFileException;
import org.brandao.entityfilemanager.LockProvider;
import org.brandao.entityfilemanager.PersistenceException;

public class EntityFileTransactionImp 
	implements EntityFileTransaction{

	protected EntityFileTransactionManager entityFileTransactionManager;
	
	protected Map<EntityFileAccess<?,?>, TransactionalEntityFileInfo<?,?>> transactionFiles;
	
	private LockProvider lockProvider;
	
	private long transactionID;
	
	private boolean started;

	private boolean rolledBack;
	
	private boolean commited;
	
	private boolean dirty;
	
	private byte status;
	
	private boolean closed;
	
	private long timeout;
	
	public EntityFileTransactionImp(
			EntityFileTransactionManager entityFileTransactionManager,
			LockProvider lockProvider,
			Map<EntityFileAccess<?,?>, TransactionalEntityFileInfo<?,?>> transactionFiles,
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
			for(TransactionalEntityFileInfo<?,?> txFile: this.transactionFiles.values()){
				txFile.getEntityFile().setTransactionStatus(EntityFileTransaction.TRANSACTION_STARTED_ROLLBACK);
			}
			
			this.status = EntityFileTransaction.TRANSACTION_STARTED_ROLLBACK;
			
			for(TransactionalEntityFileInfo<?,?> txFile: this.transactionFiles.values()){
				txFile.getEntityFile().rollback();
			}
			
			for(TransactionalEntityFileInfo<?,?> txFile: this.transactionFiles.values()){
				txFile.getEntityFile().setTransactionStatus(EntityFileTransaction.TRANSACTION_ROLLEDBACK);
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
			for(TransactionalEntityFileInfo<?,?> txFile: this.transactionFiles.values()){
				txFile.getEntityFile().setTransactionStatus(EntityFileTransaction.TRANSACTION_STARTED_COMMIT);
			}

			this.status = EntityFileTransaction.TRANSACTION_STARTED_COMMIT;
			
			for(TransactionalEntityFileInfo<?,?> txFile: this.transactionFiles.values()){
				txFile.getEntityFile().rollback();
			}
			
			for(TransactionalEntityFileInfo<?,?> txFile: this.transactionFiles.values()){
				txFile.getEntityFile().setTransactionStatus(EntityFileTransaction.TRANSACTION_COMMITED);
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

	public <T,R> long insert(T entity, EntityFileAccess<T,R> entityFileaccess)
			 throws EntityFileException {
		
		TransactionalEntityFileInfo<T,R> tei    = this.getManagedEntityFile(entityFileaccess);
		TransactionalEntityFile<T,R> entityFile = tei.getEntityFile();
		Set<Long> managedRecords                = tei.getManagedRecords();

		long pointer;
		this.lockProvider.lock(entityFileaccess);
		try{
			pointer = entityFile.insert(entity);
		}
		catch(EntityFileException e){
			this.dirty = true;
			throw e;
		}
		catch(Throwable e){
			this.dirty = true;
			throw new EntityFileException(e);
		}
		finally{
			this.lockProvider.unlock(entityFileaccess);
		}
		
		try{
			this.lockProvider.lock(entityFileaccess, pointer);
			managedRecords.add(pointer);
			return pointer;
		}
		catch(PersistenceException e){
			this.dirty = true;
			throw e;
		}
		catch(Throwable e){
			this.dirty = true;
			throw new PersistenceException(e);
		}
	}

	public <T,R> void update(long id, T entity,
			EntityFileAccess<T,R> entityFileaccess) throws PersistenceException {
		
		TransactionalEntityFileInfo<T,R> tei    = this.getManagedEntityFile(entityFileaccess);
		TransactionalEntityFile<T,R> entityFile = tei.getEntityFile();
		Set<Long> managedRecords                = tei.getManagedRecords();
		
		if(!managedRecords.contains(id)){
			
		}
		try{
			i
			this.lockProvider.tryLock(entityFileaccess, id, this.timeout, TimeUnit.MILLISECONDS);
			TransactionalEntityFile<T,R> txEntityFileAccess = this.getManagedEntityFile(entityFileaccess);
			txEntityFileAccess.update(id, entity);
		}
		catch(PersistenceException e){
			this.dirty = true;
			throw e;
		}
		catch(Throwable e){
			this.dirty = true;
			throw new PersistenceException(e);
		}
	}

	public <T,R> void delete(long id, EntityFileAccess<T,R> entityFileaccess)
			throws PersistenceException {
		try{
			this.lockProvider.tryLock(entityFileaccess, id, this.timeout, TimeUnit.MILLISECONDS);
			TransactionalEntityFile<T,R> txEntityFileAccess = this.getManagedEntityFile(entityFileaccess);
			txEntityFileAccess.delete(id);
		}
		catch(PersistenceException e){
			this.dirty = true;
			throw e;
		}
		catch(Throwable e){
			this.dirty = true;
			throw new PersistenceException(e);
		}
	}

	public <T,R> T select(long id, EntityFileAccess<T,R> entityFileaccess) {
		TransactionalEntityFile<T,R> txEntityFileAccess = this.getManagedEntityFile(entityFileaccess);
		return txEntityFileAccess.select(id);
	}

	public <T,R> T select(long id, boolean lock, EntityFileAccess<T,R> entityFileaccess) {
		try{
			this.lockProvider.tryLock(entityFileaccess, id, this.timeout, TimeUnit.MILLISECONDS);
			TransactionalEntityFile<T,R> txEntityFileAccess = this.getManagedEntityFile(entityFileaccess);
			return txEntityFileAccess.select(id);
		}
		catch(PersistenceException e){
			this.dirty = true;
			throw e;
		}
		catch(Throwable e){
			this.dirty = true;
			throw new PersistenceException(e);
		}
	}
	
	public void close() throws TransactionException{
		entityFileTransactionManager.closeTransaction(this);
	}
	
	/* private methods */
	
	@SuppressWarnings("unchecked")
	private <T,R> TransactionalEntityFileInfo<T,R> getManagedEntityFile( 
			EntityFileAccess<T,R> entityFile) throws PersistenceException{
		try{
			
			TransactionalEntityFileInfo<T,R> tx = 
				(TransactionalEntityFileInfo<T, R>) this.transactionFiles.get(entityFile);
			
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

	/* restrict methods */
	
	public Map<EntityFileAccess<?,?>, TransactionalEntityFileInfo<?,?>> getTransactionalEntityFile(){
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
