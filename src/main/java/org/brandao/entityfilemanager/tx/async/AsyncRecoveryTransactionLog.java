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
	
	private Thread confirmTransactionThread;
	
	public AsyncRecoveryTransactionLog(String name, File path, EntityFileTransactionManagerConfigurer eftmc){
		super(name, path, eftmc);
		this.confirmTransactionTask = new ConfirmTransactionTask();
		this.efam                   = new ConcurrentHashMap<EntityFileAccess<?,?,?>, AsyncAutoFlushVirutalEntityFileAccess<?,?,?>>();
		this.confirmTransactionThread = 
				new Thread(
						null,
						this.confirmTransactionTask,
						"Async recovery transaction log confirmation task");
	}

	public ConcurrentMap<EntityFileAccess<?,?,?>, AsyncAutoFlushVirutalEntityFileAccess<?, ?, ?>> getEntityFileAccessMapping(){
		return efam;
	}
	
	protected void createNewFileTransactionLog() throws Throwable{
		File f = transactionFile.getFile();
		super.createNewFileTransactionLog();
		confirmTransactionTask.transactionFiles.put(f);
	}

	public void open() throws TransactionException{
		super.open();
		this.confirmTransactionThread.start();
	}
	
	public void close() throws TransactionException{
		
		super.close();
		
		try{
			//coloca na fila de processamento o ultimo arquivo de log gerado
			confirmTransactionTask.transactionFiles.put(transactionFile.getFile());
			
			//aguarda o fim do processamento dos arquivos
			while(!confirmTransactionTask.transactionFiles.isEmpty() && confirmTransactionTask.failTransactionFileLog == null){
				Thread.sleep(100);
			}
			
			//termina o processo que confirma as transações
			confirmTransactionThread.interrupt();
			
			//aguarda o término do processo
			while(confirmTransactionTask.run){
				Thread.sleep(100);
			}
			
			//verifica se o erro é InterruptedException, indicando o encerramento do processo.
			if(!(confirmTransactionTask.failTransactionFileLog instanceof InterruptedException)){
				//se for encontrada outra exceção, ela será lançada.
				throw new TransactionException(confirmTransactionTask.failTransactionFileLog);
			}
		}
		catch(TransactionException e){
			throw e;
		}
		catch(Throwable e){
			throw new TransactionException(e);
		}
		
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
					flushVirutalEntityFileAccess();
					execute();
				}
				catch(Throwable e){
					failTransactionFileLog = e;
				}
			}
			run = false;
		}
		
		private void execute() throws Throwable {
			File file = transactionFiles.poll(1, TimeUnit.SECONDS);

			if(file == null){
				return;
			}
			
			FileAccess fa = null;
			try{
				fa                     = new FileAccess(file);
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
		
		private void flushVirutalEntityFileAccess() throws IOException{
			lock.lock();
			try{
				if(transactionFiles.isEmpty() && transactionInProgress == 0 && transactionFile.isEmpty()){
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
			}
			finally{
				lock.unlock();
			}
		}
	
	}
	
}
