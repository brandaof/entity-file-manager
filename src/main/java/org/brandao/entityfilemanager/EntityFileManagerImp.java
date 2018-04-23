package org.brandao.entityfilemanager;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.brandao.entityfilemanager.tx.EntityFileTransaction;
import org.brandao.entityfilemanager.tx.EntityFileTransactionManager;
import org.brandao.entityfilemanager.tx.EntityFileTransactionHandler;
import org.brandao.entityfilemanager.tx.EntityFileTransactionWrapper;
import org.brandao.entityfilemanager.tx.TransactionException;

public class EntityFileManagerImp 
	implements EntityFileManagerConfigurer{

	public static final String TRANSACTION_PATH = "/tx";

	public static final String DATA_PATH = "/data";
	
	private String pathName;
	
	private File path;
	
	private Map<String, EntityFileAccess<?,?>> entities;

	private File transactionPath;
	
	private File dataPath;
	
	private EntityFileTransactionManager transactioManager;
	
	private boolean started;
	
	public EntityFileManagerImp(){
		this.entities = new HashMap<String, EntityFileAccess<?,?>>();
		this.started  = false;
	}
	
	public void setPathName(String pathName) {
		this.pathName        = pathName;
		this.path            = new File(this.pathName);
		this.transactionPath = new File(this.path, TRANSACTION_PATH);
		this.dataPath        = new File(this.path, DATA_PATH);
		
		if(!this.path.exists())
			this.path.mkdirs();
		
		if(!this.transactionPath.exists())
			this.transactionPath.mkdirs();

		if(!this.dataPath.exists())
			this.dataPath.mkdirs();
		
	}

	public String getPathName() {
		return this.pathName;
	}

	public void start() throws EntityFileManagerException{
		
		if(this.started)
			throw new EntityFileManagerException("manager has been started");
		
		try{
			this.started = true;
			this.clearTransactions();
		}
		catch(EntityFileManagerException ex){
			this.started = false;
			throw ex;
		}
		catch(Throwable ex){
			this.started = false;
			throw new EntityFileManagerException(ex);
		}
	}

	public void destroy() throws EntityFileManagerException{
		
		if(!this.started)
			throw new EntityFileManagerException("manager not started");

		try{
			this.clearTransactions();
			
			for(EntityFileAccess<?,?> entityFile: this.entities.values()){
				entityFile.close();
			}
			
			this.started = false;
		}
		catch(Throwable e){
			throw new EntityFileManagerException(e);
		}
		
	}
	
	private void clearTransactions() throws EntityFileManagerException{
		
		File[] txList = this.transactionPath.listFiles();
		
		for(File txFile: txList){
			try{
				EntityFileTransactionHandler tx = null;
				try{
					tx = this.transactioManager.open(txFile.getName());
					tx.rollback();
				}
				finally{
					this.transactioManager.close(tx);
				}
			}
			catch(Throwable e){
				throw new EntityFileManagerException(e);
			}
		}
	}
	
	public void create(String name, EntityFileAccess<?,?> entityFile) throws EntityFileManagerException{
		
		try{
			if(entityFile.exists())
				entityFile.open();
			else
				entityFile.createNewFile();
			
			this.entities.put(name, entityFile);
		}
		catch(Throwable e){
			throw new EntityFileManagerException(e);
		}
	}

	public void remove(String name) throws EntityFileManagerException{
		
		EntityFileAccess<?> entity = this.entities.get(name);
		
		try{
			entity.close();
		}
		catch(Throwable e){
			throw new EntityFileManagerException(e);
		}
	}

	@SuppressWarnings("unchecked")
	public <T> EntityFile<T> getEntityFile(String name, Class<T> type){
		
		if(!this.started)
			throw new EntityFileManagerException("manager not started");
		
		EntityFileAccess<T> fileAccess = (EntityFileAccess<T>) this.entities.get(name); 
		return new EntityFileTransactionWrapper<T>(fileAccess, this.transactioManager);
	}

	public File getPath() {
		return this.path;
	}

	public File getTransactionPath() {
		return this.transactionPath;
	}

	public File getDataPath() {
		return this.dataPath;
	}

	public void setEntityFileTransactionManager(EntityFileTransactionManager transactioMmanager){
		this.transactioManager = transactioMmanager;
	}

	public EntityFileTransactionManager getEntityFileTransactionManager(){
		return this.transactioManager;
	}
	
	public void truncate(String name) throws EntityFileManagerException {
		
		try{
			EntityFileAccess<?> entity = this.entities.get(name);
			entity.createNewFile();
		}
		catch(Throwable e){
			throw new EntityFileManagerException(e);
		}
		
	}

	public EntityFileTransaction beginTransaction() throws TransactionException{
		
		EntityFileTransactionHandler tx = this.transactioManager.getCurrent();
		
		if(tx != null){
			throw new TransactionException("transaction has been strated!");
		}
		
		tx = this.transactioManager.open(UUID.randomUUID().toString());
		
		return tx;
	}

}
