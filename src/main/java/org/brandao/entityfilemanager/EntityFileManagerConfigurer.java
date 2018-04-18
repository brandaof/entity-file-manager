package org.brandao.entityfilemanager;

import java.io.File;

import org.brandao.entityfilemanager.tx.EntityFileTransactionFactory;

public interface EntityFileManagerConfigurer 
	extends EntityFileManager{

	void setTransactionFactory(EntityFileTransactionFactory factory);

	EntityFileTransactionFactory getTransactionFactory();
	
	void setPathName(String pathName);

	String getPathName();
	
	File getPath();

	File getTransactionPath();

	File getDataPath();
	
	void create(String name, EntityFile<?> entityFile, boolean override) throws EntityFileManagerException;
	
	void remove(String name) throws EntityFileManagerException;
	
	void truncate(String name) throws EntityFileManagerException;
	
	void start() throws EntityFileManagerException;
	
	void destroy() throws EntityFileManagerException;
	
}
