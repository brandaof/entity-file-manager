package org.brandao.entityfilemanager.tx;

import java.io.File;

import org.brandao.entityfilemanager.EntityFileManager;
import org.brandao.entityfilemanager.tx.EntityFileTransactionUtil.TransactionFileNameMetadata;

public class TransactionLoader {

	public static EntityFileTransaction[] loadTransactions(
			EntityFileManager entityFileManager, File txPath){
		File[] txList = txPath.listFiles();
		
		for(File f: txList){
			
			if(!f.getName().endsWith(EntityFileTransactionUtil.EXTENSION)){
				continue;
			}
		
			TransactionFileNameMetadata tfn = 
				EntityFileTransactionUtil.getTransactionFileNameMetadata(f);
		}
		
	}
	
}
