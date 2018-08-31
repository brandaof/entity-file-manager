package org.brandao.entityfilemanager.tx;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;

import org.brandao.entityfilemanager.EntityFileAccess;
import org.brandao.entityfilemanager.FileAccess;

public class RecoveryLog {

	private static final long MIN_FILELOG_LENGTH = 25*1024*1024;
	
	private TransactionFileLog transactionFile;

	private long limitFileLength;

	private TransactionFileCreator transactionFileCreator;
	
	private Map<TransactionFileLog, LinkedHashSet<Long>> transactions;
	
	private TransactionReader reader;
	
	private TransactionWritter writter;
	
	private EntityFileTransactionManagerConfigurer eftmc;
	
	public RecoveryLog(String name, File path, EntityFileTransactionManagerConfigurer eftmc){
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

	public synchronized void registerTransaction(ConfigurableEntityFileTransaction ceft)
			throws TransactionException {
		
		try{
			if(isEmptyTransaction(ceft)){
				return;
			}
			
			if(transactionFile.length() > this.limitFileLength){
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
		
		if(isEmptyTransaction(ceft)){
			return;
		}
		
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
		
		transactionFile.close();
		
		File nextFile   = this.transactionFileCreator.getNextFile();
		FileAccess fa   = new FileAccess(nextFile, new RandomAccessFile(nextFile, "rw"));
		transactionFile = new TransactionFileLog(fa, reader, writter, eftmc);
		
		transactionFile.load();
	}

	public void open() throws TransactionException{
		
		File txf = transactionFileCreator.getCurrentFile();
		
		if(txf == null){
			
			txf = transactionFileCreator.getNextFile();
			try{
				FileAccess fa   = new FileAccess(txf, new RandomAccessFile(txf, "rw"));
				transactionFile = new TransactionFileLog(fa, reader, writter, eftmc);
				transactionFile.load();
				return;
			}
			catch(Throwable e){
				throw new TransactionException(e);
			}
			
		}

		try{
			reloadTransactions(eftmc);
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
			
			FileAccess fa = new FileAccess(txf, new RandomAccessFile(txf, "rw"));
			TransactionFileLog transactionFile = new TransactionFileLog(fa, reader, writter, eftmc);
			try{
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
				transactionFile.close();
				transactionFile.delete();
			}
		}
		
	}

}
