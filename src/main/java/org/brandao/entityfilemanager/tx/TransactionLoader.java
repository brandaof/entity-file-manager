package org.brandao.entityfilemanager.tx;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.brandao.entityfilemanager.EntityFileAccess;
import org.brandao.entityfilemanager.EntityFileManagerConfigurer;
import org.brandao.entityfilemanager.LockProvider;

public class TransactionLoader {

	private static final String TX_FILE_PATTERN = "^ftx\\-(.*)$";
	
	private static final Pattern PATTERN_TX_FILE;
	
	static{
		PATTERN_TX_FILE = Pattern.compile(TX_FILE_PATTERN);
	}
	
	public ConfigurableEntityFileTransaction[] loadTransactions(
			LockProvider lockProvider,
			EntityFileManagerConfigurer entityFileManager, 
			EntityFileTransactionManagerConfigurer transactionManager, File txPath
			) throws TransactionException, IOException{
		
		File[] files = txPath.listFiles();
		
		File[] txFiles = this.getTransactionFiles(files);
		
		TransactionFileNameMetadata[] txfmd = this.toTransactionFileNameMetadata(txFiles);
		
		return this.toEntityFileTransaction(txfmd, transactionManager, lockProvider);
		
		/*
		Map<Long, List<TransactionFileNameMetadata>> mappedTXFMD = this.groupTransaction(txfmd);
		
		Map<Long, Map<EntityFileAccess<?,?,?>, TransactionEntityFileAccess<?,?,?>>> transactionFiles =
				this.toTransactionalEntityFile(mappedTXFMD, entityFileManager);
		
		return toEntityFileTransaction(transactionFiles, 
				transactionManager, entityFileManager);
		*/
	}
	
	private File[] getTransactionFiles(File[] value){
		List<File> tmp = new ArrayList<File>();
		
		for(File f: value){

			if(!f.getName().matches(TX_FILE_PATTERN)){
				continue;
			}
			
			tmp.add(f);
		}
		
		return tmp.toArray(new File[0]);
	}
	
	private TransactionFileNameMetadata[] toTransactionFileNameMetadata(File[] values) throws TransactionException{
		TransactionFileNameMetadata[] result = new TransactionFileNameMetadata[values.length];
		
		for(int i=0;i<values.length;i++){
			File f = values[i];
			result[i] = this.getTransactionFileNameMetadata(f);
		}
		
		return result;
	}
	
	private TransactionFileNameMetadata getTransactionFileNameMetadata(File f) throws TransactionException{
	    Matcher m = PATTERN_TX_FILE.matcher(f.getName());
	    
	    if(!m.find()){
	    	throw new TransactionException("invalid file name: " + f.getName());
	    }
	    
	    String transactionID = m.group(1); 
		TransactionFileNameMetadata r = 
				new TransactionFileNameMetadata(
					"ftx", 
					Long.parseLong(transactionID, Character.MAX_RADIX), f);
		
		return r;
	}
	
	private ConfigurableEntityFileTransaction[] toEntityFileTransaction(
			TransactionFileNameMetadata[] files, 
			EntityFileTransactionManagerConfigurer entityFileTransactionManagerConfigurer,
			LockProvider lockProvider) throws TransactionException{
		
		ConfigurableEntityFileTransaction[] r = new ConfigurableEntityFileTransaction[files.length];
		int i = 0;
		for(TransactionFileNameMetadata f: files){
			try{
				ConfigurableEntityFileTransaction c = this.toEntityFileTransaction(f);
				c.setEntityFileTransactionManagerConfigurer(entityFileTransactionManagerConfigurer);
				c.setLockProvider(lockProvider);
				r[i++] = c;
			}
			catch(Throwable e){
				throw new TransactionException("fail load transaction: " + f.getTransactionID(), e);
			}
		}
		
		return r;
	}

	public void writeEntityFileTransaction(
			ConfigurableEntityFileTransaction t, File path) throws IOException{
		
		File f = new File(path, "ftx-" + Long.toString(t.getTransactionID(), Character.MAX_RADIX));
		
		FileOutputStream fout = null;
		ObjectOutput out = null;
		try{
			fout = new FileOutputStream(f);
			out  = new ObjectOutputStream(fout);
			out.writeObject(t);
		}
		finally{
			if(fout != null){
				fout.close();
			}
		}
		
	}

	public void deleteEntityFileTransaction(
			ConfigurableEntityFileTransaction t, File path) throws IOException{
		File f = new File(path, "ftx-" + Long.toString(t.getTransactionID(), Character.MAX_RADIX));
		f.delete();
	}
	
	private ConfigurableEntityFileTransaction toEntityFileTransaction(TransactionFileNameMetadata f
			) throws IOException, ClassNotFoundException{
		FileInputStream fin = null;
		ObjectInput in = null;
		try{
			fin = new FileInputStream(f.getFile());
			in = new ObjectInputStream(fin);
			return (ConfigurableEntityFileTransaction) in.readObject();
		}
		finally{
			if(fin != null){
				fin.close();
			}
		}
		
	}
	
	private Map<Long, List<TransactionFileNameMetadata>> groupTransaction(
			TransactionFileNameMetadata[] values){
		
		Map<Long, List<TransactionFileNameMetadata>> result = 
				new HashMap<Long, List<TransactionFileNameMetadata>>();
		
		for(TransactionFileNameMetadata t: values){
			long txID = t.getTransactionID();
			
			List<TransactionFileNameMetadata> list = result.get(txID);
			
			if(list == null){
				list = new ArrayList<TransactionFileNameMetadata>();
				result.put(txID, list);
			}
			
			list.add(t);
		}
		
		return result;
	}
	
	private Map<Long, Map<EntityFileAccess<?,?,?>, TransactionEntityFileAccess<?,?,?>>> toTransactionalEntityFile(
			Map<Long, List<TransactionFileNameMetadata>> values, 
			EntityFileManagerConfigurer entityFileManager) throws TransactionException, IOException{
		
		Map<Long, Map<EntityFileAccess<?,?,?>, TransactionEntityFileAccess<?,?,?>>> result = 
				new HashMap<Long, Map<EntityFileAccess<?,?,?>,TransactionEntityFileAccess<?,?,?>>>();
		
		for(Entry<Long, List<TransactionFileNameMetadata>> entry: values.entrySet()){
			long txID = entry.getKey();
			
			Map<EntityFileAccess<?,?,?>,TransactionEntityFileAccess<?,?,?>> map = result.get(txID);
			
			if(map == null){
				map = new HashMap<EntityFileAccess<?,?,?>, TransactionEntityFileAccess<?,?,?>>();
				result.put(txID, map);
			}
			
			for(TransactionFileNameMetadata tfmd: entry.getValue()){
				EntityFileAccess<?,?,?> efa = entityFileManager.getEntityFile(tfmd.getName());
				
				if(efa == null){
					throw new TransactionException("entity file not found: " + tfmd.getName());
				}

				TransactionEntityFileAccess<?, ?, ?> tef = EntityFileTransactionUtil
						.getTransactionEntityFileAccess(efa, tfmd);
				
				if(tef == null || !tef.exists()){
					throw new TransactionException("transaction data corrupted: " + txID);
				}
				
				tef.open();
				
				map.put(efa, tef);
			}
		}

		
		return result;
	}

	private ConfigurableEntityFileTransaction[] toEntityFileTransaction(
			Map<Long, Map<EntityFileAccess<?,?,?>, TransactionEntityFileAccess<?,?,?>>> values,
			EntityFileTransactionManagerImp transactioManager, EntityFileManagerConfigurer manager) throws IOException, TransactionException{

		ConfigurableEntityFileTransaction[] result = new ConfigurableEntityFileTransaction[values.size()];
		
		int i = 0;
		
		for(Entry<Long, Map<EntityFileAccess<?,?,?>, TransactionEntityFileAccess<?,?,?>>> entry: values.entrySet()){
			
			long transactionID = entry.getKey();
			
			Map<EntityFileAccess<?,?,?>, TransactionEntityFileAccess<?,?,?>> transactionFiles =
					entry.getValue();
			
			byte transactionStatus    = this.getCurrentTransactionStatus(transactionFiles);
			byte transactionIsolation = this.getTransactionIsolation(transactionFiles);
			boolean started           = (transactionStatus & EntityFileTransaction.TRANSACTION_NOT_STARTED) != 0;
			boolean rolledBack        = (transactionStatus & EntityFileTransaction.TRANSACTION_ROLLEDBACK) != 0;
			boolean commited          = (transactionStatus & EntityFileTransaction.TRANSACTION_COMMITED) != 0;
			
			ConfigurableEntityFileTransaction eft = 
				transactioManager.load(transactionFiles, transactionStatus, 
						transactionID, transactionIsolation, started, rolledBack, commited);
			
			result[i++] = eft;
		}
		
		
		return result;
	}
	
	private byte getCurrentTransactionStatus(
			Map<EntityFileAccess<?,?,?>, TransactionEntityFileAccess<?,?,?>> map
			) throws IOException, TransactionException{
		
		byte mergedTransactionStatus = EntityFileTransactionUtil.mergeTransactionStatus(map);
		
		if(mergedTransactionStatus == 0){
			throw new TransactionException("invalid transaction status: " + mergedTransactionStatus);
		}

		byte status = EntityFileTransactionUtil.getTransactionStatus(mergedTransactionStatus);
		
		if(status == 0){
			throw new TransactionException("invalid transaction status: " + mergedTransactionStatus);
		}
		
		return status;
	}

	private byte getTransactionIsolation(
			Map<EntityFileAccess<?,?,?>, TransactionEntityFileAccess<?,?,?>> map
			) throws IOException, TransactionException{
		
		return EntityFileTransactionUtil.getTransactionIsolation(map);
	}
	
}
