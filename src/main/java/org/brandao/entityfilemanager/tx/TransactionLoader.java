package org.brandao.entityfilemanager.tx;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.brandao.entityfilemanager.EntityFileAccess;
import org.brandao.entityfilemanager.EntityFileManager;
import org.brandao.entityfilemanager.tx.EntityFileTransactionUtil.TransactionFileNameMetadata;

public class TransactionLoader {

	public EntityFileTransaction[] loadTransactions(
			EntityFileManager entityFileManager, File txPath){
		
		File[] files = txPath.listFiles();
		File[] txFiles = this.getTransactionFiles(files);
		TransactionFileNameMetadata[] txfmd = this.toTransactionFileNameMetadata(txFiles);
		Map<Long, List<TransactionFileNameMetadata>> mappedTXFMD = this.groupTransaction(txfmd);
		
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
	
	private Map<Long, Map<EntityFileAccess<?,?>, TransactionalEntityFile<?,?>>> toTransactionalEntityFile(
			Map<Long, List<TransactionFileNameMetadata>> values, EntityFileManager entityFileManager){
		
		Map<Long, Map<EntityFileAccess<?,?>, TransactionalEntityFile<?,?>>> result = 
				new HashMap<Long, Map<EntityFileAccess<?,?>,TransactionalEntityFile<?,?>>>();
		
		for(Entry<Long, List<TransactionFileNameMetadata>> entry: values.entrySet()){
			long txID = entry.getKey();
			
			Map<EntityFileAccess<?,?>,TransactionalEntityFile<?,?>> map = result.get(txID);
			
			if(map == null){
				map = new HashMap<EntityFileAccess<?,?>, TransactionalEntityFile<?,?>>();
				result.put(txID, map);
			}
			
			
		}

		
		return result;
	}
	
}
