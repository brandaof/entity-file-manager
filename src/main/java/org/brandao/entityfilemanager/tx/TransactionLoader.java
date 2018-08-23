package org.brandao.entityfilemanager.tx;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.brandao.entityfilemanager.EntityFileAccess;
import org.brandao.entityfilemanager.EntityFileManagerConfigurer;

public class TransactionLoader {

	public EntityFileTransaction[] loadTransactions(
			EntityFileManagerConfigurer entityFileManager, 
			EntityFileTransactionManagerImp transactionManager, File txPath
			) throws TransactionException, IOException{
		
		File[] files = txPath.listFiles();
		
		File[] txFiles = this.getTransactionFiles(files);
		
		TransactionFileNameMetadata[] txfmd = this.toTransactionFileNameMetadata(txFiles);
		
		Map<Long, List<TransactionFileNameMetadata>> mappedTXFMD = this.groupTransaction(txfmd);
		
		Map<Long, Map<EntityFileAccess<?,?,?>, TransactionEntityFileAccess<?,?,?>>> transactionFiles =
				this.toTransactionalEntityFile(mappedTXFMD, entityFileManager);
		
		return toEntityFileTransaction(transactionFiles, 
				transactionManager, entityFileManager);
	}
	
	private File[] getTransactionFiles(File[] value){
		List<File> tmp = new ArrayList<File>();
		
		for(File f: value){

			if(!f.getName().endsWith(EntityFileTransactionUtil.EXTENSION)){
				continue;
			}
			
			tmp.add(f);
		}
		
		return tmp.toArray(new File[0]);
	}
	
	private TransactionFileNameMetadata[] toTransactionFileNameMetadata(File[] values){
		TransactionFileNameMetadata[] result = new TransactionFileNameMetadata[values.length];
		
		for(int i=0;i<values.length;i++){
			File f = values[i];
			result[i] = EntityFileTransactionUtil.getTransactionFileNameMetadata(f);
		}
		
		return result;
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
						.getTransactionEntityFileAccess(efa);
				
				if(tef == null || !tef.exists()){
					throw new TransactionException("transaction data corrupted: " + txID);
				}
				
				tef.open();
				
				map.put(efa, tef);
			}
		}

		
		return result;
	}

	private EntityFileTransaction[] toEntityFileTransaction(
			Map<Long, Map<EntityFileAccess<?,?,?>, TransactionEntityFileAccess<?,?,?>>> values,
			EntityFileTransactionManagerImp transactioManager, EntityFileManagerConfigurer manager) throws IOException, TransactionException{

		EntityFileTransaction[] result = new EntityFileTransaction[values.size()];
		
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
			
			EntityFileTransaction eft = 
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
