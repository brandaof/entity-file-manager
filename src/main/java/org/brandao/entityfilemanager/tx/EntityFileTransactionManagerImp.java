package org.brandao.entityfilemanager.tx;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.brandao.entityfilemanager.EntityFileAccess;
import org.brandao.entityfilemanager.EntityFileManagerConfigurer;
import org.brandao.entityfilemanager.EntityFileManagerException;
import org.brandao.entityfilemanager.LockProvider;
import org.brandao.entityfilemanager.tx.readcommited.ReadCommitedTransactionalEntityFile;

public class EntityFileTransactionManagerImp 
	implements EntityFileTransactionManagerConfigurer{

	public static final String TRANSACTION_PATH = "/tx";
	
	public static final long DEFAULT_TIMEOUT = 5*60*1000;
	
	private long transactionIDCounter;
	
	private Lock txIDLock;
	
	private ConcurrentMap<Long, ConfigurableEntityFileTransaction> transactions;
	
	private long timeout;
	
	private File transactionPath;
	
	private EntityFileManagerConfigurer entityFileManagerConfigurer;
	
	private LockProvider lockProvider;
	
	public EntityFileTransactionManagerImp(){
		this.transactionIDCounter = 0;
		this.txIDLock             = new ReentrantLock();
		this.transactions         = new ConcurrentHashMap<Long, ConfigurableEntityFileTransaction>();
	}
	
	private long getNextTransactionID() {
		
		long currentTransactionID;
		
		this.txIDLock.lock();
		try{
			currentTransactionID = this.transactionIDCounter++;
		}
		finally{
			this.txIDLock.unlock();
		}
		return currentTransactionID;
	}

	public LockProvider getLockProvider() {
		return this.lockProvider;
	}

	public void setLockProvider(LockProvider value) {
		this.lockProvider = value;
	}
	
	public void setEntityFileManagerConfigurer(EntityFileManagerConfigurer value) {
		this.entityFileManagerConfigurer = value;
	}

	public EntityFileManagerConfigurer getEntityFileManagerConfigurer() {
		return this.entityFileManagerConfigurer;
	}
	
	public File getTransactionPath() {
		return transactionPath;
	}

	public void setTransactionPath(File transactionPath) {
		this.transactionPath = transactionPath;
		
		if(!this.transactionPath.exists()){
			this.transactionPath.mkdirs();
		}
	}

	public long getTimeout() {
		return timeout;
	}

	public void setTimeout(long timeout) {
		this.timeout = timeout;
	}

	public void init() throws TransactionException{
		this.reloadTransactions();
	}
	
	public void destroy() throws TransactionException{
		this.closeAllTransactions();
	}
	
	public EntityFileTransaction openTransaction() throws TransactionException {
		long txID = this.getNextTransactionID();
		ConfigurableEntityFileTransaction tx = 
			new ReadCommitedEntityFileTransaction(
				this, this.lockProvider,
				new HashMap<EntityFileAccess<?,?,?>, TransactionEntity<?,?>>(), 
				EntityFileTransaction.TRANSACTION_NOT_STARTED, 
				txID, true, false, false, this.timeout);
		
		this.transactions.put(txID, tx);
		
		return tx;
	}

	public void closeTransaction(ConfigurableEntityFileTransaction transaction) throws TransactionException {
		
		ConfigurableEntityFileTransaction tx = (ConfigurableEntityFileTransaction)transaction;
		
		if(tx.isCommited() || tx.isRolledBack() || tx.isClosed() || !tx.isStarted()){
			return;
		}
		
		tx.rollback();
		tx.setClosed(true);
		
		this.transactions.remove(tx.getTransactionID());
	}

	public void commitTransaction(ConfigurableEntityFileTransaction transaction) throws TransactionException {
		
	}

	public void rollbackTransaction(ConfigurableEntityFileTransaction transaction) throws TransactionException {
		
	}
	
	private void reloadTransactions() throws EntityFileManagerException{
		try{
			TransactionLoader txLoader = new TransactionLoader();
			ConfigurableEntityFileTransaction[] txList = 
					txLoader.loadTransactions(this.entityFileManagerConfigurer, 
							this, this.transactionPath);
			
			for(ConfigurableEntityFileTransaction tx: txList){
				this.closeTransaction(tx);
			}
			
		}
		catch(Throwable e){
			throw new EntityFileManagerException(e);
		}
	}

	private void closeAllTransactions() throws EntityFileManagerException{
		try{
			for(ConfigurableEntityFileTransaction tx: this.transactions.values()){
				this.closeTransaction(tx);
			}
		}
		catch(Throwable e){
			throw new EntityFileManagerException(e);
		}
	}
	
	public ConfigurableEntityFileTransaction load(
			Map<EntityFileAccess<?,?,?>, TransactionEntityFileAccess<?,?,?>> transactionFiles,
			byte status, long transactionID, byte transactionIsolation, boolean started, 
			boolean rolledBack,	boolean commited) throws TransactionException {
		
		if(transactionIsolation != EntityFileTransaction.TRANSACTION_READ_COMMITED){
			throw new TransactionException("transaction not supported: " + transactionIsolation);
		}
		
		return this.loadReadCommitedEntityFileTransaction(
				transactionFiles, status, transactionID, transactionIsolation, 
				started, rolledBack, commited);
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private ConfigurableEntityFileTransaction loadReadCommitedEntityFileTransaction(
			Map<EntityFileAccess<?,?,?>, TransactionEntityFileAccess<?,?,?>> transactionFiles,
			byte status, long transactionID, byte transactionIsolation, boolean started, 
			boolean rolledBack,	boolean commited){
		
		Map<EntityFileAccess<?,?,?>, TransactionEntity<?,?>> tf =
			new HashMap<EntityFileAccess<?,?,?>, TransactionEntity<?,?>>();
		
		for(Entry<EntityFileAccess<?,?,?>, TransactionEntityFileAccess<?,?,?>> entry: 
			transactionFiles.entrySet()){
			
			tf.put(
				entry.getKey(), 
				new ReadCommitedTransactionalEntityFile(
						entry.getValue(), this.lockProvider, this.timeout)
			);
			
		}
		
		return new ReadCommitedEntityFileTransaction(
				this, this.lockProvider,
				tf, 
				status, 
				transactionID, started, rolledBack, commited, this.timeout);
		
	}
	
}
