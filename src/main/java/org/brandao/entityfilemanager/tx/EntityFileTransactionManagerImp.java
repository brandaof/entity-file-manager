package org.brandao.entityfilemanager.tx;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
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
import org.brandao.entityfilemanager.TransactionLog;
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

	//private TransactionLog transactionLog;
	
	private RecoveryLog recoveryLog;
	
	public EntityFileTransactionManagerImp(){
		this.transactionIDCounter = 0;
		this.txIDLock             = new ReentrantLock();
		this.transactions         = new ConcurrentHashMap<Long, ConfigurableEntityFileTransaction>();
		//this.transactionLog       = null;
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
		
		this.recoveryLog = new RecoveryLog("recovery", transactionPath, this);
		
		//if(transactionLog == null){
		//	transactionLog = new TransactionLogImp("binlog", transactionPath);
		//}
		
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
				txID, true, false, false, this.timeout, false);
		
		this.transactions.put(txID, tx);
		
		return tx;
	}

	public void closeTransaction(ConfigurableEntityFileTransaction transaction) throws TransactionException {
		
		ConfigurableEntityFileTransaction tx = (ConfigurableEntityFileTransaction)transaction;
		
		if(tx.isCommited() || tx.isRolledBack() || tx.isClosed() || !tx.isStarted()){
			return;
		}
		
		if(tx.getStatus() == EntityFileTransaction.TRANSACTION_STARTED_COMMIT){
			tx.commit();
		}
		else
		if(tx.getStatus() == EntityFileTransaction.TRANSACTION_STARTED_ROLLBACK){
			tx.rollback();
		}
		
		try{
			//apaga os dados da transação.
			this.confirmTransactionInformation(transaction);

			tx.setClosed(true);
			
			this.transactions.remove(tx.getTransactionID());
		}
		catch(Throwable e){
			throw new TransactionException(e);
		}	
		
	}

	public void commitTransaction(ConfigurableEntityFileTransaction transaction) throws TransactionException {
		
		if(transaction.isDirty()){
			throw new TransactionException("transaction rollback is needed");
		}
		
		if(transaction.isRolledBack()){
			throw new TransactionException("transaction has been rolled back");
		}
		
		if(transaction.isCommited()){
			throw new TransactionException("transaction has been commited");
		}

		if(transaction.isClosed()){
			throw new TransactionException("transaction has been closed");
		}
		
		if(!transaction.isStarted()){
			throw new TransactionException("transaction not started");
		}
		
		
		try{
			
			if(transaction.isEmpty()){
				transaction.setCommited(true);
				transaction.setRolledBack(false);
				return;
			}
			
			//obtém os arquivos envolvidos na transação
			Map<EntityFileAccess<?,?,?>, TransactionEntity<?,?>> transactionFiles =
					transaction.getTransactionFiles();
			Collection<TransactionEntity<?,?>> transactionEntity = transactionFiles.values();
			
			//marca o início do rollback
			this.updateTransactionStatus(transaction, transactionEntity, EntityFileTransaction.TRANSACTION_STARTED_COMMIT);

			//salva a transação em mídia persistente para possível recuperação pós-falha.
			this.registerTransactionInformation(transaction, false);
			
			//executa o rollback
			this.executeCommit(transaction, transactionEntity);
			
			//atualiza o status da transação para finalizada.
			this.updateTransactionStatus(transaction, transactionEntity, EntityFileTransaction.TRANSACTION_COMMITED);
			
			//Registra a transação no log de transações
			this.logTransaction(transaction);
			
			//apaga os dados da transação.
			this.confirmTransactionInformation(transaction);
			
			transaction.setCommited(true);
			transaction.setRolledBack(false);
		}
		catch(TransactionException e){
			throw e;
		}		
		catch(Throwable e){
			throw new TransactionException(e);
		}		
	}

	private void executeCommit(ConfigurableEntityFileTransaction transaction, 
			Collection<TransactionEntity<?,?>> transactionEntity) throws IOException, TransactionException{
		
		for(TransactionEntity<?,?> txFile: transactionEntity){
			txFile.commit();
		}
		
	}
	
	public void rollbackTransaction(ConfigurableEntityFileTransaction transaction) throws TransactionException {
		
		//verifica o status atual da transação
		
		if(transaction.isClosed()){
			throw new TransactionException("transaction has been closed");
		}
		
		if(transaction.isRolledBack()){
			throw new TransactionException("transaction has been rolled back");
		}

		if(transaction.isCommited()){
			throw new TransactionException("transaction has been commited");
		}
		
		if(!transaction.isStarted()){
			throw new TransactionException("transaction not started");
		}
		
		try{
			
			if(transaction.isEmpty()){
				transaction.setCommited(false);
				transaction.setRolledBack(true);
				return;
			}
			
			//obtém os arquivos envolvidos na transação
			Map<EntityFileAccess<?,?,?>, TransactionEntity<?,?>> transactionFiles =
					transaction.getTransactionFiles();
			Collection<TransactionEntity<?,?>> transactionEntity = transactionFiles.values();
			
			//marca o início do rollback
			this.updateTransactionStatus(transaction, transactionEntity, EntityFileTransaction.TRANSACTION_STARTED_ROLLBACK);
			
			//salva a transação em mídia persistente para possível recuperação pós-falha.
			this.registerTransactionInformation(transaction, false);
			
			//executa o rollback
			this.executeRollback(transaction, transactionEntity);
			
			//atualiza o status da transação para finalizada.
			this.updateTransactionStatus(transaction, transactionEntity, EntityFileTransaction.TRANSACTION_ROLLEDBACK);
			
			//Registra a transação no log de transações
			this.logTransaction(transaction);
			
			//apaga os dados da transação.
			this.confirmTransactionInformation(transaction);
			
			transaction.setCommited(false);
			transaction.setRolledBack(true);
		}
		catch(TransactionException e){
			throw e;
		}
		catch(Throwable e){
			throw new TransactionException(e);
		}
	}
	
	protected void executeRollback(ConfigurableEntityFileTransaction transaction, 
			Collection<TransactionEntity<?,?>> transactionEntity) throws IOException, TransactionException{
		
		for(TransactionEntity<?,?> txFile: transactionEntity){
			txFile.rollback();
		}
		
	}
	
	protected void updateTransactionStatus(ConfigurableEntityFileTransaction transaction, 
			Collection<TransactionEntity<?,?>> transactionEntity, byte transactionStatus) throws IOException{

		for(TransactionEntity<?,?> txFile: transactionEntity){
			txFile.setTransactionStatus(transactionStatus);
		}
		
		transaction.setStatus(transactionStatus);
		
	}

	protected void registerTransactionInformation(
			ConfigurableEntityFileTransaction transaction, boolean override) throws TransactionException {
		if(override || !transaction.isRecoveredTransaction()){
			recoveryLog.registerTransaction(transaction);
		}
	}

	protected void confirmTransactionInformation(
			ConfigurableEntityFileTransaction transaction) throws TransactionException{
		if(!transaction.isRecoveredTransaction()){
			recoveryLog.deleteTransaction(transaction);
		}
	}
	
	protected void logTransaction(ConfigurableEntityFileTransaction transaction){
	}
	
	protected void reloadTransactions() throws EntityFileManagerException{
		try{
			recoveryLog.open();
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
	
	public <T,R,H> TransactionEntity<T,R> createTransactionalEntity(
			EntityFileAccess<T,R,H> entityFile, long transactionID,	byte transactionIsolation) throws TransactionException{
		
		if(transactionIsolation != EntityFileTransaction.TRANSACTION_READ_COMMITED){
			throw new TransactionException("transaction not supported: " + transactionIsolation);
		}
		
		TransactionEntityFileAccess<T, R, H> txFile = 
				this.createTransactionEntityFileAccess(entityFile, transactionID, 
						transactionIsolation);
		
		return 
			new ReadCommitedTransactionalEntityFile<T, R, H>(
					txFile, this.lockProvider, this.timeout);
	}

	public <T,R,H> TransactionEntityFileAccess<T, R, H> createTransactionEntityFileAccess(
			EntityFileAccess<T,R,H> entityFile, long transactionID,	byte transactionIsolation) throws TransactionException{
		
		try{
			TransactionEntityFileAccess<T, R, H> tefa =
				new TransientTransactionEntityFileAccess<T, R, H>(
				//new TransactionEntityFileAccess<T, R, H>(
						entityFile, 
						new File(entityFile.getAbsolutePath() + "_" + Long.toString(transactionID, Character.MAX_RADIX)), 
						transactionID, 
						transactionIsolation);
			
			tefa.createNewFile();
			return tefa;
		}
		catch(Throwable e){
			throw new TransactionException(e);
		}
	}
	
	public <T,R,H> TransactionEntity<T,R> createTransactionalEntity(
			TransactionEntityFileAccess<T,R,H> transactionEntityFile, long transactionID,
			byte transactionIsolation, long timeout) throws TransactionException{
		
		if(transactionIsolation != EntityFileTransaction.TRANSACTION_READ_COMMITED){
			throw new TransactionException("transaction not supported: " + transactionIsolation);
		}
		
		return new ReadCommitedTransactionalEntityFile<T, R, H>(
					transactionEntityFile, this.lockProvider, timeout < 0? this.timeout : timeout);
	}

	public ConfigurableEntityFileTransaction load(
			Map<EntityFileAccess<?,?,?>, TransactionEntityFileAccess<?,?,?>> transactionFiles,
			byte status, long transactionID, byte transactionIsolation, boolean started, 
			boolean rolledBack,	boolean commited, long timeout) throws TransactionException {
		
		if(transactionIsolation != EntityFileTransaction.TRANSACTION_READ_COMMITED){
			throw new TransactionException("transaction not supported: " + transactionIsolation);
		}
		
		return this.loadReadCommitedEntityFileTransaction(
				transactionFiles, status, transactionID, transactionIsolation, 
				started, rolledBack, commited, timeout);
	}
	
	private ConfigurableEntityFileTransaction loadReadCommitedEntityFileTransaction(
			Map<EntityFileAccess<?,?,?>, TransactionEntityFileAccess<?,?,?>> transactionFiles,
			byte status, long transactionID, byte transactionIsolation, boolean started, 
			boolean rolledBack,	boolean commited, long timeout) throws TransactionException{
		
		Map<EntityFileAccess<?,?,?>, TransactionEntity<?,?>> tf =
			new HashMap<EntityFileAccess<?,?,?>, TransactionEntity<?,?>>();
		
		for(Entry<EntityFileAccess<?,?,?>, TransactionEntityFileAccess<?,?,?>> entry: 
			transactionFiles.entrySet()){
			
			TransactionEntity<?,?> te =
					this.createTransactionalEntity(
							entry.getValue(), transactionID, 
							transactionIsolation, timeout);
			
			tf.put(entry.getKey(), te);
			
		}
		
		return new ReadCommitedEntityFileTransaction(
				this, this.lockProvider,
				tf, 
				status, 
				transactionID, started, rolledBack, commited, timeout, true);
		
	}

	public void setTransactionLog(TransactionLog value) {
		// TODO Auto-generated method stub
		
	}

	public TransactionLog getTransactionLog() {
		// TODO Auto-generated method stub
		return null;
	}
	
}
