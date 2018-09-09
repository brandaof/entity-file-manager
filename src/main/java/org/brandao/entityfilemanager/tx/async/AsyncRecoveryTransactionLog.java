package org.brandao.entityfilemanager.tx.async;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

import org.brandao.entityfilemanager.EntityFileAccess;
import org.brandao.entityfilemanager.FileAccess;
import org.brandao.entityfilemanager.tx.ConfigurableEntityFileTransaction;
import org.brandao.entityfilemanager.tx.EntityFileTransactionManagerConfigurer;
import org.brandao.entityfilemanager.tx.RecoveryTransactionLog;
import org.brandao.entityfilemanager.tx.RecoveryTransactionLogImp;
import org.brandao.entityfilemanager.tx.TransactionException;
import org.brandao.entityfilemanager.tx.TransactionFileLog;

public class AsyncRecoveryTransactionLog
	extends RecoveryTransactionLogImp 
	implements RecoveryTransactionLog{

	private ConcurrentMap<EntityFileAccess<?,?,?>, AsyncAutoFlushVirutalEntityFileAccess<?, ?, ?>> efam;
	
	private ConfirmTransactionTask confirmTransactionTask;
	
	private long transactionInProgress;
	
	private Thread confirmTransactionThread;
	
	private long timeFlush = TimeUnit.SECONDS.toMillis(1);
	
	private long lastFlush;
	
	public AsyncRecoveryTransactionLog(String name, File path, EntityFileTransactionManagerConfigurer eftmc){
		super(name, path, eftmc);
		this.transactionInProgress  = 0;
		this.confirmTransactionTask = new ConfirmTransactionTask();
		this.efam                   = new ConcurrentHashMap<EntityFileAccess<?,?,?>, AsyncAutoFlushVirutalEntityFileAccess<?,?,?>>();
		this.lastFlush              = System.currentTimeMillis();
		this.confirmTransactionThread = 
				new Thread(
						null,
						this.confirmTransactionTask,
						"Async recovery transaction log confirmation task");
		
		this.confirmTransactionThread.start();
	}

	public ConcurrentMap<EntityFileAccess<?,?,?>, AsyncAutoFlushVirutalEntityFileAccess<?, ?, ?>> getEntityFileAccessMapping(){
		return efam;
	}
	
	public void registerTransaction(ConfigurableEntityFileTransaction ceft)
			throws TransactionException {
		
		lock.lock();
		try{
			//Cria um novo arquivo de log se for atingido o tamanho máximo
			if(transactionFile.getFilelength() > this.limitFileLength){
				createNewFileTransactionLog();
			}
			
			ceft.setRepository(transactionFile);
			transactionFile.add(ceft);
			ceft.setRecoveredTransaction(true);
			transactionInProgress++;
		}
		catch(Throwable e){
			throw new TransactionException(e);
		}
		finally{
			lock.unlock();
		}
	}

	public void deleteTransaction(ConfigurableEntityFileTransaction ceft)
			throws TransactionException {
		
		lock.lock();
		try{
			//Cria um novo arquivo de log quando não existir transações abertas 
			//forçando a atualização dos arquivos envolvidos. 
			transactionInProgress--;
			
			try{
				if(transactionInProgress == 0 && (System.currentTimeMillis() - lastFlush) > timeFlush){
					createNewFileTransactionLog();
					lastFlush = System.currentTimeMillis();
					flushVirutalEntityFileAccess();
				}
			}
			catch(Throwable e){
				throw new TransactionException(e);
			}
		}
		finally{
			lock.unlock();
		}
		
	}
	
	protected void createNewFileTransactionLog() throws Throwable{
		File f = transactionFile.getFile();
		super.createNewFileTransactionLog();
		confirmTransactionTask.transactionFiles.put(f);
	}

	private void flushVirutalEntityFileAccess() throws IOException{
		lock.lock();
		try{
			for(AsyncAutoFlushVirutalEntityFileAccess<?, ?, ?> afvefa: efam.values()){
				Lock lock = afvefa.getLock();
				lock.lock();
				try{
					afvefa.tryResync();
				}
				finally{
					lock.unlock();
				}
			}
		}
		finally{
			lock.unlock();
		}
	}
	
	public void close() throws TransactionException{
		confirmTransactionThread.interrupt();
		
		while(confirmTransactionTask.run){
			try{
				Thread.sleep(100);
			}
			catch(Throwable e){
				throw new TransactionException(e);
			}
		}
		
		if(!(confirmTransactionTask.failTransactionFileLog instanceof InterruptedException)){
			throw new TransactionException(confirmTransactionTask.failTransactionFileLog);
		}

		super.close();
		
	}
	
	public long getTimeFlush() {
		return timeFlush;
	}

	public void setTimeFlush(long timeFlush) {
		this.timeFlush = timeFlush;
	}

	private class ConfirmTransactionTask implements Runnable{

		public BlockingQueue<File> transactionFiles;
		
		public volatile Throwable failTransactionFileLog;
		
		public boolean run;
		
		public ConfirmTransactionTask() {
			this.transactionFiles = new LinkedBlockingQueue<File>();
			this.failTransactionFileLog = null;
			this.run = false;
		}
	
		public void run() {
			run = true;
			while(failTransactionFileLog == null){
				try{
					execute();
				}
				catch(Throwable e){
					failTransactionFileLog = e;
				}
			}
			run = false;
		}
		
		private void execute() throws Throwable {
			File file = transactionFiles.take();

			FileAccess fa = null;
			try{
				fa              = new FileAccess(file);
				TransactionFileLog tfl = new TransactionFileLog(fa, reader, writter, eftmc);
				tfl.load();
				
				while(tfl.hasMoreElements()){
					ConfigurableEntityFileTransaction ceft = tfl.nextElement();
					ceft.setStarted(true);
					eftmc.closeTransaction(ceft);
				}
				
				if(tfl.getError() != null){
					throw tfl.getError();
				}
				
				tfl.close();
				tfl.delete();
			}
			catch(Throwable e){
				try{
					if(fa != null){
						fa.close();
					}
				}
				catch(Throwable x){
					throw new TransactionException(x.toString(), e);
				}
				
				throw new TransactionException(e);
			}
			
		}
	
	}
	
}
