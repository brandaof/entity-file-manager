package org.brandao.entityfilemanager.tx;

import java.io.IOException;
import java.util.Map;

import org.brandao.entityfilemanager.EntityFileAccess;
import org.brandao.entityfilemanager.EntityFileException;
import org.brandao.entityfilemanager.LockProvider;

public abstract class AbstractEntityFileTransaction 
	implements ConfigurableEntityFileTransaction{

	private static final long serialVersionUID = 7555768592401042353L;

	protected transient EntityFileTransactionManagerConfigurer entityFileTransactionManager;
	
	protected Map<EntityFileAccess<?,?,?>, TransactionEntity<?,?>> transactionFiles;
	
	protected transient LockProvider lockProvider;
	
	protected long transactionID;
	
	protected boolean started;

	protected boolean rolledBack;
	
	protected boolean commited;
	
	protected boolean dirty;
	
	protected byte status;
	
	protected boolean closed;
	
	protected long timeout;

	protected boolean registered;
	
	protected byte transactionIsolation;
	
	protected boolean recoveredTransaction;
	
	protected TransactionFileLog transactionFileLog;
	
	public AbstractEntityFileTransaction(
			EntityFileTransactionManagerConfigurer entityFileTransactionManager,
			LockProvider lockProvider,
			Map<EntityFileAccess<?,?,?>, TransactionEntity<?,?>> transactionFiles,
			byte status, long transactionID, byte transactionIsolation, boolean started, boolean rolledBack, 
			boolean commited, long timeout, boolean recoveredTransaction) {
		this.entityFileTransactionManager = entityFileTransactionManager;
		this.recoveredTransaction         = recoveredTransaction;
		this.transactionIsolation         = transactionIsolation;
		this.transactionFiles             = transactionFiles;
		this.transactionID                = transactionID;
		this.lockProvider                 = lockProvider;
		this.rolledBack                   = rolledBack;
		this.commited                     = commited;
		this.timeout                      = timeout;
		this.started                      = started;
		this.status                       = status;
		this.dirty                        = false;
		this.registered                   = false;
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

	@SuppressWarnings("rawtypes")
	public boolean isEmpty() throws IOException{
		
		for(TransactionEntity<?,?> t: transactionFiles.values()){
			TransactionEntityFileAccess tefa = t.getTransactionEntityFileAccess();
			if(tefa.length() != 0){
				return false;
			}
		}
		
		return true;
	}
	
	public void rollback() throws TransactionException {
		entityFileTransactionManager.rollbackTransaction(this);
	}

	public void commit() throws TransactionException {
		entityFileTransactionManager.commitTransaction(this);
	}

	public void close() throws TransactionException{
		entityFileTransactionManager.closeTransaction(this);
	}
	
	public void setRepository(TransactionFileLog value) {
		transactionFileLog = value;
	}

	public TransactionFileLog getRepository() {
		return transactionFileLog;
	}
	
	public <T, R, H> long insert(T entity, EntityFileAccess<T, R, H> entityFileAccess)
			throws EntityFileException {
		try{
			TransactionEntity<T,R> tei = this.getManagedEntityFile(entityFileAccess);
			return tei.insert(entity);
		}
		catch(Throwable e){
			this.dirty = true;
			throw e instanceof EntityFileException? 
					(EntityFileException)e : 
					new EntityFileException(e);
		}
	}

	public <T, R, H> long[] insert(T[] entity,
			EntityFileAccess<T, R, H> entityFileAccess) throws EntityFileException {
		try{
			TransactionEntity<T,R> tei = this.getManagedEntityFile(entityFileAccess);
			return tei.insert(entity);
		}
		catch(Throwable e){
			this.dirty = true;
			throw e instanceof EntityFileException? 
					(EntityFileException)e : 
					new EntityFileException(e);
		}
	}

	public <T, R, H> void update(long id, T entity,
			EntityFileAccess<T, R, H> entityFileAccess) throws EntityFileException {
		try{
			TransactionEntity<T,R> tei = this.getManagedEntityFile(entityFileAccess);
			tei.update(id, entity);
		}
		catch(Throwable e){
			this.dirty = true;
			throw e instanceof EntityFileException? 
					(EntityFileException)e : 
					new EntityFileException(e);
		}
	}

	public <T, R, H> void update(long[] id, T[] entity,
			EntityFileAccess<T, R, H> entityFileAccess) throws EntityFileException {
		try{
			TransactionEntity<T,R> tei = this.getManagedEntityFile(entityFileAccess);
			tei.update(id, entity);
		}
		catch(Throwable e){
			this.dirty = true;
			throw e instanceof EntityFileException? 
					(EntityFileException)e : 
					new EntityFileException(e);
		}
	}

	public <T, R, H> void delete(long id, EntityFileAccess<T, R, H> entityFileAccess)
			throws EntityFileException {
		try{
			TransactionEntity<T,R> tei = this.getManagedEntityFile(entityFileAccess);
			tei.delete(id);
		}
		catch(Throwable e){
			this.dirty = true;
			throw e instanceof EntityFileException? 
					(EntityFileException)e : 
					new EntityFileException(e);
		}
	}

	public <T, R, H> void delete(long[] id, EntityFileAccess<T, R, H> entityFileAccess)
			throws EntityFileException {
		try{
			TransactionEntity<T,R> tei = this.getManagedEntityFile(entityFileAccess);
			tei.delete(id);
		}
		catch(Throwable e){
			this.dirty = true;
			throw e instanceof EntityFileException? 
					(EntityFileException)e : 
					new EntityFileException(e);
		}
	}

	public <T, R, H> T select(long id, EntityFileAccess<T, R, H> entityFileAccess)
			throws EntityFileException {
		try{
			TransactionEntity<T,R> tei = this.getManagedEntityFile(entityFileAccess);
			return tei.select(id);
		}
		catch(Throwable e){
			this.dirty = true;
			throw e instanceof EntityFileException? 
					(EntityFileException)e : 
					new EntityFileException(e);
		}
	}

	public <T, R, H> T[] select(long[] id, EntityFileAccess<T, R, H> entityFileAccess)
			throws EntityFileException {
		try{
			TransactionEntity<T,R> tei = this.getManagedEntityFile(entityFileAccess);
			return tei.select(id);
		}
		catch(Throwable e){
			this.dirty = true;
			throw e instanceof EntityFileException? 
					(EntityFileException)e : 
					new EntityFileException(e);
		}
	}

	public <T, R, H> T select(long id, boolean lock,
			EntityFileAccess<T, R, H> entityFileAccess) throws EntityFileException {
		try{
			TransactionEntity<T,R> tei = this.getManagedEntityFile(entityFileAccess);
			return tei.select(id, lock);
		}
		catch(Throwable e){
			this.dirty = true;
			throw e instanceof EntityFileException? 
					(EntityFileException)e : 
					new EntityFileException(e);
		}
	}

	public <T, R, H> T[] select(long[] id, boolean lock,
			EntityFileAccess<T, R, H> entityFileAccess) throws EntityFileException {
		TransactionEntity<T,R> tei = this.getManagedEntityFile(entityFileAccess);
		return tei.select(id, lock);
	}
	
	public <T, R, H> long length(EntityFileAccess<T, R, H> entityFileAccess)
			throws EntityFileException {
		try{
			TransactionEntity<T,R> tei = this.getManagedEntityFile(entityFileAccess);
			return tei.length();
		}
		catch(Throwable e){
			this.dirty = true;
			throw e instanceof EntityFileException? 
					(EntityFileException)e : 
					new EntityFileException(e);
		}
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
			
			tx = entityFileTransactionManager.getEntityFileTransactionFactory()
					.createTransactionalEntity(entityFile, transactionID, transactionIsolation, lockProvider, timeout);
			
			this.transactionFiles.put(entityFile, tx);
			return tx;
		}
		catch(Throwable e){
			throw new EntityFileException(e);
		}
	}

	/* restrict methods */
	
	public Map<EntityFileAccess<?,?,?>, TransactionEntity<?,?>> getTransactionalEntityFile(){
		return this.transactionFiles;
	}
	
	public Map<EntityFileAccess<?,?,?>, TransactionEntity<?,?>> getTransactionFiles(){
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
	
	public void setStatus(byte value){
		this.status = value;
	}
	
	public void setRolledBack(boolean value){
		this.rolledBack = value;
	}
	
	public void setCommited(boolean value){
		this.commited = value;
	}
	
	public void setStarted(boolean value) {
		this.started = value;
	}

	public void setEntityFileTransactionManagerConfigurer(
			EntityFileTransactionManagerConfigurer entityFileTransactionManager){
		this.entityFileTransactionManager = entityFileTransactionManager;
	}
	
	public void setLockProvider(LockProvider lockProvider){
		this.lockProvider = lockProvider;
	}

	public void setTransactionID(byte value) {
		this.transactionIsolation = value;
	}

	public void setDirty(boolean value) {
		this.dirty = value;
	}

	public void setRecoveredTransaction(boolean value){
		this.recoveredTransaction = value;
	}
	
	public boolean isRecoveredTransaction(){
		return this.recoveredTransaction;
	}
	
	public byte getTransactionIsolation() {
		return this.transactionIsolation;
	}

	public void setRegistered(boolean value) {
		registered = value;
	}

	public boolean isRegistered() {
		return registered;
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
