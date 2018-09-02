package org.brandao.entityfilemanager.tx.async;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.brandao.entityfilemanager.FileAccess;
import org.brandao.entityfilemanager.tx.ConfigurableEntityFileTransaction;
import org.brandao.entityfilemanager.tx.EntityFileTransactionManagerConfigurer;
import org.brandao.entityfilemanager.tx.RecoveryTransactionLog;
import org.brandao.entityfilemanager.tx.TransactionException;
import org.brandao.entityfilemanager.tx.TransactionFileCreator;
import org.brandao.entityfilemanager.tx.TransactionFileLog;
import org.brandao.entityfilemanager.tx.TransactionReader;
import org.brandao.entityfilemanager.tx.TransactionWritter;

public class AsyncRecoveryTransactionLog 
	implements RecoveryTransactionLog{

	//private static final long MIN_FILELOG_LENGTH = 300*1024*1024;
	private static final long MIN_FILELOG_LENGTH = 100;
	
	private TransactionFileLog transactionFile;

	private long limitFileLength;

	private TransactionFileCreator transactionFileCreator;
	
	private TransactionReader reader;
	
	private TransactionWritter writter;
	
	private EntityFileTransactionManagerConfigurer eftmc;
	
	private ConfirmTransactionTask confirmTransactionTask;
	
	public AsyncRecoveryTransactionLog(String name, File path, EntityFileTransactionManagerConfigurer eftmc){
		this.transactionFileCreator = new TransactionFileCreator(name, path);
		this.limitFileLength        = MIN_FILELOG_LENGTH;
		this.reader                 = new TransactionReader();
		this.writter                = new TransactionWritter();
		this.eftmc                  = eftmc;
		this.confirmTransactionTask = new ConfirmTransactionTask(eftmc);
		
		Thread confirmTransactionTask = 
				new Thread(
						null,
						this.confirmTransactionTask,
						"Async recovery transaction log confirmation task");
		
		confirmTransactionTask.start();
	}

	public void setLimitFileLength(long value) throws TransactionException {
		
		if(value < MIN_FILELOG_LENGTH){
			throw new TransactionException("invalid length: " + value);
		}
		
		this.limitFileLength = value;
	}

	public long getLimitFileLength() {
		return limitFileLength;
	}

	public void close() throws TransactionException{
		try{
			if(transactionFile != null){
				transactionFile.close();
			}
		}
		catch(Throwable e){
			throw new TransactionException(e);
		}
	}
	
	public synchronized void registerTransaction(ConfigurableEntityFileTransaction ceft)
			throws TransactionException {
		
		try{
			if(transactionFile.getFilelength() > this.limitFileLength){
				createNewFileTransactionLog();
			}
			
			ceft.setRepository(transactionFile);
			transactionFile.add(ceft);
			
		}
		catch(Throwable e){
			throw new TransactionException(e);
		}
	}

	public synchronized void deleteTransaction(ConfigurableEntityFileTransaction ceft)
			throws TransactionException {
	}
	
	private void createNewFileTransactionLog(
			) throws TransactionException, IOException, InterruptedException{
		
		confirmTransactionTask.transactionFiles.put(transactionFile);
		
		File nextFile = this.transactionFileCreator.getNextFile();
		FileAccess fa = null;
		try{
			fa              = new FileAccess(nextFile);
			transactionFile = new TransactionFileLog(fa, reader, writter, eftmc);
			transactionFile.load();
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

	public void open() throws TransactionException{
		
		File txf = transactionFileCreator.getCurrentFile();
		
		if(txf == null){
			
			txf = transactionFileCreator.getNextFile();
			FileAccess fa = null;
			try{
				fa              = new FileAccess(txf);
				transactionFile = new TransactionFileLog(fa, reader, writter, eftmc);
				transactionFile.load();
				return;
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
		else{
	
			FileAccess fa = null;
			try{
				reloadTransactions(eftmc);
				transactionFileCreator.setIndex(0);
				
				txf             = transactionFileCreator.getNextFile();
				fa              = new FileAccess(txf);
				transactionFile = new TransactionFileLog(fa, reader, writter, eftmc);
				transactionFile.load();
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
				
				throw e instanceof TransactionException? 
						(TransactionException)e : 
						new TransactionException(e);
			}
			
		}
	}
	
	private void reloadTransactions(EntityFileTransactionManagerConfigurer eftmc
			) throws FileNotFoundException, IOException, TransactionException{
		
		int index = transactionFileCreator.getCurrentIndex();
		File txf = null;
		
		for(int i=0;i<=index;i++){
			
			txf = transactionFileCreator.getFileByIndex(i);
			
			if(!txf.exists()){
				continue;
			}
			
			FileAccess fa = null;			
			try{
				fa                                 = new FileAccess(txf);
				TransactionFileLog transactionFile = new TransactionFileLog(fa, reader, writter, eftmc);
				transactionFile.load();
				
				if(transactionFile.getError() != null){
					throw new TransactionException("transaction file error: " + txf.getName(), transactionFile.getError());
				}
				
				while(transactionFile.hasMoreElements()){
					ConfigurableEntityFileTransaction ceft = transactionFile.nextElement();
					eftmc.closeTransaction(ceft);
				}
				
			}
			finally{
				if(fa != null){
					fa.close();
					fa.delete();
				}
			}
		}
		
	}

	private class ConfirmTransactionTask implements Runnable{

		public BlockingQueue<TransactionFileLog> transactionFiles;
		
		public EntityFileTransactionManagerConfigurer eftmc;
		
		public volatile Throwable failTransactionFileLog;
		
		public ConfirmTransactionTask(EntityFileTransactionManagerConfigurer eftmc) {
			this.transactionFiles = new LinkedBlockingQueue<TransactionFileLog>();
			this.eftmc = eftmc;
			this.failTransactionFileLog = null;
		}
	
		public void run() {
			while(failTransactionFileLog == null){
				try{
					execute();
				}
				catch(Throwable e){
					failTransactionFileLog = e;
				}
			}
		}
		
		public void execute() throws Throwable {
			TransactionFileLog tfl = transactionFiles.take();
			
			tfl.reset();
			
			while(tfl.hasMoreElements()){
				ConfigurableEntityFileTransaction ceft = tfl.nextElement();
				eftmc.closeTransaction(ceft);
			}
			
			if(tfl.getError() != null){
				throw tfl.getError();
			}
			
			tfl.close();
			tfl.delete();
		}
	
	}
	
}
