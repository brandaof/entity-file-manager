package org.brandao.entityfilemanager.tx.async;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
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

	private ConcurrentMap<EntityFileAccess<?,?,?>, AutoFlushVirutalEntityFileAccess<?, ?, ?>> efam;
	
	private ConfirmTransactionTask confirmTransactionTask;
	
	private long transactionInProgress;
	
	public AsyncRecoveryTransactionLog(String name, File path, EntityFileTransactionManagerConfigurer eftmc){
		super(name, path, eftmc);
		this.transactionInProgress  = 0;
		this.confirmTransactionTask = new ConfirmTransactionTask();
		this.efam                   = new ConcurrentHashMap<EntityFileAccess<?,?,?>, AutoFlushVirutalEntityFileAccess<?,?,?>>();
		
		Thread confirmTransactionTask = 
				new Thread(
						null,
						this.confirmTransactionTask,
						"Async recovery transaction log confirmation task");
		
		confirmTransactionTask.start();
	}

	public ConcurrentMap<EntityFileAccess<?,?,?>, AutoFlushVirutalEntityFileAccess<?, ?, ?>> getEntityFileAccessMapping(){
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
				if(transactionInProgress == 0){
					createNewFileTransactionLog();
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

	private class ConfirmTransactionTask implements Runnable{

		public BlockingQueue<File> transactionFiles;
		
		public volatile Throwable failTransactionFileLog;
		
		public ConfirmTransactionTask() {
			this.transactionFiles = new LinkedBlockingQueue<File>();
			this.failTransactionFileLog = null;
		}
	
		public void run() {
			while(failTransactionFileLog == null){
				try{
					flushVirutalEntityFileAccess();
					execute();
				}
				catch(Throwable e){
					failTransactionFileLog = e;
				}
			}
		}
		
		private void flushVirutalEntityFileAccess() throws IOException{
			lock.lock();
			try{
				if(transactionFiles.isEmpty() && transactionInProgress == 0){
					for(AutoFlushVirutalEntityFileAccess<?, ?, ?> afvefa: efam.values()){
						Lock lock = afvefa.getLock();
						lock.lock();
						try{
							afvefa.reset();
						}
						finally{
							lock.unlock();
						}
					}
					
				}
			}
			finally{
				lock.unlock();
			}
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
