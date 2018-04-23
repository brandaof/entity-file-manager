package org.brandao.entityfilemanager;

import java.io.File;

import org.brandao.entityfilemanager.tx.EntityFileTransactionManager;

public interface EntityFileManagerConfigurer 
	extends EntityFileManager{

	void setEntityFileTransactionManager(EntityFileTransactionManager factory);

	EntityFileTransactionManager getEntityFileTransactionManager();
	
	void setLockProvider(LockProvider provider);
	
	LockProvider getLockProvider();
	
	void setPathName(String pathName);

	String getPathName();
	
	File getPath();

	File getTransactionPath();

	File getDataPath();
	
	void create(String name, EntityFileAccess<?,?> entityFile) throws EntityFileManagerException;
	
	void remove(String name) throws EntityFileManagerException;
	
	void truncate(String name) throws EntityFileManagerException;
	
	void start() throws EntityFileManagerException;
	
	void destroy() throws EntityFileManagerException;
	
}
