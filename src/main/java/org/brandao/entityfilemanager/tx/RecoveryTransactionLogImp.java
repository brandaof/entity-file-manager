package org.brandao.entityfilemanager.tx;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;

import org.brandao.entityfilemanager.FileAccess;

public class RecoveryTransactionLogImp {

	private static final long MIN_FILELOG_LENGTH = 25*1024*1024;
	
	private TransactionFileLog transactionFile;

	private long limitFileLength;

	private TransactionFileCreator transactionFileCreator;
	
	private Map<TransactionFileLog, LinkedHashSet<Long>> transactions;
	
	private TransactionReader reader;
	
	private TransactionWritter writter;
	
	private EntityFileTransactionManagerConfigurer eftmc;
	
	public RecoveryTransactionLogImp(String name, File path, EntityFileTransactionManagerConfigurer eftmc){
		this.transactionFileCreator = new TransactionFileCreator(name, path);
		this.limitFileLength        = MIN_FILELOG_LENGTH;
		this.transactions           = new HashMap<TransactionFileLog, LinkedHashSet<Long>>();
		this.reader                 = new TransactionReader();
		this.writter                = new TransactionWritter();
		this.eftmc                  = eftmc;
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

	public void close() throws IOException{
		if(transactionFile != null){
			transactionFile.close();
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
	}

	public synchronized void deleteTransaction(ConfigurableEntityFileTransaction ceft)
			throws TransactionException {
		
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
	
	private void createNewFileTransactionLog() throws TransactionException, IOException{
		
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

}
