package org.brandao.entityfilemanager.tx;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Collection;
import java.util.Map;

import org.brandao.entityfilemanager.EntityFileAccess;
import org.brandao.entityfilemanager.FileAccess;
import org.brandao.entityfilemanager.TransactionLog;

public class TransactionLogImp 
	implements TransactionLog{

	private static final long MIN_FILELOG_LENGTH = 25*1024*1024;
	
	private FileAccess transactionFile;

	private long limitFileLength;

	private TransactionFileCreator transactionFileCreator;
	
	private TransactionWritter transactionWritter;
	
	private TransactionReader transactionReader;
	
	public TransactionLogImp(String name, File path){
		this.transactionFileCreator = new TransactionFileCreator(name, path);
		this.limitFileLength        = MIN_FILELOG_LENGTH;
		this.transactionWritter     = new TransactionWritter();
		this.transactionReader      = new TransactionReader();
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
			if(isEmptyTransaction(ceft)){
				return;
			}
			
			if(transactionFile.length() > this.limitFileLength){
				createNewFileTransactionLog();
			}
			
			transactionFile.writeLong(transactionFile.getFilePointer() + 8);
			transactionWritter.write(ceft, transactionFile);
			
		}
		catch(Throwable e){
			throw new TransactionException(e);
		}
	}

	@SuppressWarnings("rawtypes")
	private boolean isEmptyTransaction(ConfigurableEntityFileTransaction ceft) throws IOException{
		Map<EntityFileAccess<?,?,?>, TransactionEntity<?,?>> m = ceft.getTransactionFiles();
		Collection<TransactionEntity<?,?>> list = m.values();
		
		boolean empty = true;
		
		for(TransactionEntity<?,?> tt: list){
			TransactionEntityFileAccess tefa = tt.getTransactionEntityFileAccess();
			empty = empty && tefa.length() == 0;
			
			if(!empty){
				break;
			}
		}
		
		return empty;
	}
	
	private void createNewFileTransactionLog() throws IOException{
		
		transactionFile.flush();
		transactionFile.close();
		
		File nextFile   = this.transactionFileCreator.getNextFile();
		transactionFile = new FileAccess(nextFile, new RandomAccessFile(nextFile, "rw"));
		
		transactionFile.seek(0);
	}

	public void open(EntityFileTransactionManagerConfigurer eftmc) throws TransactionException{
		
		File txf = transactionFileCreator.getCurrentFile();
		
		if(txf == null){
			
			txf = transactionFileCreator.getNextFile();
			try{
				transactionFile = new FileAccess(txf, new RandomAccessFile(txf, "rw"));
				return;
			}
			catch(Throwable e){
				throw new TransactionException(e);
			}
			
		}

		try{
			reloadTransactions(eftmc);
			transactionFile = new FileAccess(txf, new RandomAccessFile(txf, "rw"));
			transactionFile.seek(transactionFile.length());
		}
		catch(TransactionException e){
			throw e;
		}
		catch(Throwable e){
			throw new TransactionException(e);
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
			
			FileAccess tfa = new FileAccess(txf, new RandomAccessFile(txf, "rw"));
			
			try{
				if(tfa.length() == 0){
					
					if(i == index){
						continue;
					}
					
					throw new TransactionException("invalid transaction file size: " + txf.getName());
				}
				
				reloadTransactionFile(tfa, eftmc);
			}
			finally{
				tfa.close();
			}
		}
		
		transactionFile = new FileAccess(txf, new RandomAccessFile(txf, "rw"));
		
		if(transactionFile.length() > limitFileLength){
			createNewFileTransactionLog();
		}
		else{
			transactionFile.seek(transactionFile.length());
		}
		
	}

	private void reloadTransactionFile(FileAccess tfa, EntityFileTransactionManagerConfigurer eftmc
			) throws TransactionException, IOException{
		
		ConfigurableEntityFileTransaction tx;
		long lastPointerValue = -1;
		
		do{
			
			long pointerValue = tfa.readLong();
			
			if(pointerValue != tfa.getFilePointer()){
				throw new TransactionException("invalid transaction: " + tfa.getFile().getName());
			}
			
			try{
				tx = transactionReader.read(eftmc, tfa);
				eftmc.closeTransaction(tx);
			}
			catch(TransactionException e){
				throw e;
			}
			catch(Throwable e){
				throw new TransactionException("invalid transaction file: " + tfa.getFile().getName(), e);
			}
		
			lastPointerValue = pointerValue;
			
		}while(tfa.getFilePointer() != tfa.length());
		
	}
}
