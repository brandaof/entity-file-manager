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
import org.brandao.entityfilemanager.tx.EntityFileTransactionUtil.TransactionFileNameMetadata;

public class TransactionLoader {

	public EntityFileTransaction[] loadTransactions(
			EntityFileManagerConfigurer entityFileManager, File txPath
			) throws TransactionException, IOException{
		
		File[] files = txPath.listFiles();
		
		File[] txFiles = this.getTransactionFiles(files);
		
		TransactionFileNameMetadata[] txfmd = this.toTransactionFileNameMetadata(txFiles);
		
		Map<Long, List<TransactionFileNameMetadata>> mappedTXFMD = this.groupTransaction(txfmd);
		
		Map<Long, Map<EntityFileAccess<?,?>, TransactionalEntityFile<?,?>>> transactionFiles =
				this.toTransactionalEntityFile(mappedTXFMD, entityFileManager);
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
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	private Map<Long, Map<EntityFileAccess<?,?>, TransactionalEntityFile<?,?>>> toTransactionalEntityFile(
			Map<Long, List<TransactionFileNameMetadata>> values, 
			EntityFileManagerConfigurer entityFileManager) throws TransactionException, IOException{
		
		Map<Long, Map<EntityFileAccess<?,?>, TransactionalEntityFile<?,?>>> result = 
				new HashMap<Long, Map<EntityFileAccess<?,?>,TransactionalEntityFile<?,?>>>();
		
		for(Entry<Long, List<TransactionFileNameMetadata>> entry: values.entrySet()){
			long txID = entry.getKey();
			
			Map<EntityFileAccess<?,?>,TransactionalEntityFile<?,?>> map = result.get(txID);
			
			if(map == null){
				map = new HashMap<EntityFileAccess<?,?>, TransactionalEntityFile<?,?>>();
				result.put(txID, map);
			}
			
			for(TransactionFileNameMetadata tfmd: entry.getValue()){
				EntityFileAccess<?,?> efa = entityFileManager.getEntityFile(tfmd.getName());
				
				if(efa == null){
					throw new TransactionException("entity file not found: " + tfmd.getName());
				}

				TransactionEntityFileAccess<?, ?> tef = EntityFileTransactionUtil
						.getTransactionEntityFileAccess(efa, txID);
				
				if(tef == null || !tef.exists()){
					throw new TransactionException("transaction data corrupted: " + txID);
				}
				
				tef.open();
				
				TransactionalEntityFile<?,?> tnef = new TransactionalEntityFile(efa, tef);
				map.put(efa, tnef);
			}
		}

		
		return result;
	}
	
}
