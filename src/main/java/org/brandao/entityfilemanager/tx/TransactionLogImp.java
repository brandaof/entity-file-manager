package org.brandao.entityfilemanager.tx;

import java.io.File;
import java.io.IOException;

import org.brandao.entityfilemanager.FileAccess;
import org.brandao.entityfilemanager.TransactionLog;

public class TransactionLogImp 
	implements TransactionLog{

	private static final long MIN_FILELOG_LENGTH = 25*1024*1024;
	
	private TransactionFileLog transactionFile;

	private long limitFileLength;

	private TransactionFileCreator transactionFileCreator;
	
	private TransactionWritter transactionWritter;
	
	private TransactionReader transactionReader;
	
	private EntityFileTransactionManagerConfigurer eftmc;
	
	public TransactionLogImp(String name, File path, EntityFileTransactionManagerConfigurer eftmc){
		this.transactionFileCreator = new TransactionFileCreator(name, path);
		this.limitFileLength        = MIN_FILELOG_LENGTH;
		this.transactionWritter     = new TransactionWritter();
		this.transactionReader      = new TransactionReader();
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

	public synchronized void registerLog(ConfigurableEntityFileTransaction ceft)
			throws TransactionException {
		
		try{
			if(transactionFile.getFilelength() > this.limitFileLength){
				createNewFileTransactionLog();
			}
			
			transactionFile.add(ceft);
		}
		catch(TransactionException e){
			throw e;
		}
		catch(Throwable e){
			throw new TransactionException(e);
		}
	}
	
	private void createNewFileTransactionLog() throws IOException, TransactionException{
		
		transactionFile.close();
		
		FileAccess fa = null;
		try{
			File nextFile   = this.transactionFileCreator.getNextFile();
			fa              = new FileAccess(nextFile);
			transactionFile = new TransactionFileLog(fa, transactionReader, transactionWritter, eftmc);
			transactionFile.load();
			return;
		}
		catch(Throwable e){
			try{
				if(transactionFile != null){
					transactionFile.close();
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

	public void open() throws TransactionException{
		
		File txf = transactionFileCreator.getCurrentFile();
		
		if(txf == null){
			
			txf           = transactionFileCreator.getNextFile();
			FileAccess fa = null;
			try{
				fa              = new FileAccess(txf);
				transactionFile = new TransactionFileLog(fa, transactionReader, transactionWritter, eftmc);
				transactionFile.load();
				return;
			}
			catch(Throwable e){
				try{
					if(transactionFile != null){
						transactionFile.close();
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
		else{
			
			FileAccess fa = null;
			try{
				fa              = new FileAccess(txf);
				transactionFile = new TransactionFileLog(fa, transactionReader, transactionWritter, eftmc);
				transactionFile.load();
				
				if(transactionFile.getError() != null){
					throw transactionFile.getError();
				}
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

	public void close() throws TransactionException {
		try{
			transactionFile.close();
		}
		catch(Throwable e){
			throw new TransactionException(e);
		}
		
	}
	
}
