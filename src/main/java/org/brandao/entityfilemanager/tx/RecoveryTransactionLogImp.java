package org.brandao.entityfilemanager.tx;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.brandao.entityfilemanager.FileAccess;

public class RecoveryTransactionLogImp 
	implements RecoveryTransactionLog{

	private static final long MIN_FILELOG_LENGTH = 300*1024*1024;
	
	protected TransactionFileLog transactionFile;

	protected long limitFileLength;

	protected TransactionFileCreator transactionFileCreator;
	
	protected Map<TransactionFileLog, LinkedHashSet<Long>> transactions;
	
	protected TransactionReader reader;
	
	protected TransactionWritter writter;
	
	protected EntityFileTransactionManagerConfigurer eftmc;
	
	protected Lock lock;
	
	public RecoveryTransactionLogImp(String name, File path, EntityFileTransactionManagerConfigurer eftmc){
		this.transactionFileCreator = new TransactionFileCreator(name, path);
		this.limitFileLength        = MIN_FILELOG_LENGTH;
		this.transactions           = new HashMap<TransactionFileLog, LinkedHashSet<Long>>();
		this.reader                 = new TransactionReader();
		this.writter                = new TransactionWritter();
		this.eftmc                  = eftmc;
		this.lock                   = new ReentrantLock();
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
	
	public void registerTransaction(ConfigurableEntityFileTransaction ceft)
			throws TransactionException {
		
		lock.lock();
		try{
			if(transactionFile.getFilelength() > this.limitFileLength){
				createNewFileTransactionLog();
			}
			
			ceft.setRepository(transactionFile);
			transactionFile.add(ceft);
			
			LinkedHashSet<Long> txSet = transactions.get(ceft.getRepository());
			
			if(txSet == null){
				txSet = new LinkedHashSet<Long>();
				transactions.put(transactionFile, txSet);
			}
			
			txSet.add(ceft.getTransactionID());
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
			LinkedHashSet<Long> txSet = transactions.get(ceft.getRepository());
			
			if(txSet == null){
				throw new TransactionException("transaction not found: " + ceft.getTransactionID());
			}
			
			txSet.remove(ceft.getTransactionID());
			
			if(txSet.isEmpty()){
				try{
					TransactionFileLog txFile = ceft.getRepository();
					if(transactionFile == txFile){
						txFile.reset();
					}
					else{
						txFile.delete();
					}
				}
				catch(Throwable e){
					throw new TransactionException("Fail truncate recovery transaction log: " + ceft.getRepository());
				}
			}
		}
		finally{
			lock.unlock();
		}
	}
	
	protected void createNewFileTransactionLog() throws Throwable{
		
		transactionFile.close();
		
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
			
			FileAccess fa   = null;
			boolean success = false;
			try{
				fa                                 = new FileAccess(txf);
				TransactionFileLog transactionFile = new TransactionFileLog(fa, reader, writter, eftmc);
				transactionFile.load();
				
				if(transactionFile.getError() != null){
					throw new TransactionException("transaction file error: " + txf.getName(), transactionFile.getError());
				}
				
				while(transactionFile.hasMoreElements()){
					ConfigurableEntityFileTransaction ceft = transactionFile.nextElement();
					//inicia a transação
					ceft.setStarted(true);
					eftmc.closeTransaction(ceft);
				}
				success = true;
			}
			finally{
				if(fa != null){
					
					fa.close();
					
					if(success){
						fa.delete();
					}
					
				}
			}
		}
		
	}

}
