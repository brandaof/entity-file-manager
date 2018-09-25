package org.brandao.entityfilemanager.tx;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.brandao.entityfilemanager.FileAccess;

public class RecoveryTransactionLogImp 
	implements RecoveryTransactionLog{

	private static final long MIN_FILELOG_LENGTH = 300*1024*1024;
	
	protected TransactionFileLog transactionFile;

	protected long limitFileLength;

	protected TransactionFileCreator transactionFileCreator;
	
	protected TransactionReader reader;
	
	protected TransactionWritter writter;
	
	protected EntityFileTransactionManagerConfigurer eftmc;
	
	protected Lock lock;
	
	protected boolean forceReload;
	
	protected boolean closed;
	
	protected long transactionInProgress;
	
	protected long timeFlush = TimeUnit.SECONDS.toMillis(60);
	
	protected long lastFlush;
	
	public RecoveryTransactionLogImp(String name, File path, EntityFileTransactionManagerConfigurer eftmc){
		this.transactionFileCreator = new TransactionFileCreator(name, path);
		this.limitFileLength        = MIN_FILELOG_LENGTH;
		this.reader                 = new TransactionReader();
		this.writter                = new TransactionWritter();
		this.eftmc                  = eftmc;
		this.lock                   = new ReentrantLock();
		this.closed                 = true;
		this.transactionInProgress  = 0;
		this.lastFlush              = System.currentTimeMillis();
	}

	public void setForceReload(boolean value) {
		this.forceReload = value;
	}

	public boolean isForceReload() {
		return forceReload;
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
			closed = true;
			
			while(transactionInProgress != 0){
				Thread.sleep(100);
			}
			
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
		
		if(closed){
			throw new TransactionException("transaction closed");
		}
		
		lock.lock();
		try{
			//Cria um novo arquivo de log se for atingido o tamanho máximo
			if(transactionFile.getFilelength() > this.limitFileLength){
				createNewFileTransactionLog();
			}
			
			ceft.setRepository(transactionFile);
			transactionFile.add(ceft);
			transactionInProgress++;
			ceft.setRegistered(true);
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
			//Se a transação não foi registrada, não precisa ser removida.
			if(!ceft.isRegistered()){
				return;
			}
			
			//Cria um novo arquivo de log quando não existir transações abertas 
			//forçando a atualização dos arquivos envolvidos.
			transactionInProgress--;
			
			try{
				if(transactionInProgress == 0 && (System.currentTimeMillis() - lastFlush) > timeFlush){
					createNewFileTransactionLog();
					lastFlush = System.currentTimeMillis();
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
		
		closed = false;
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
					if(forceReload){
						transactionFile.cutLog();
					}
					else{
						throw new TransactionException("transaction file error: " + txf.getName(), transactionFile.getError());
					}
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
