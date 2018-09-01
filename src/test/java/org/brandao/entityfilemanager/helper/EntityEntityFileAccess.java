package org.brandao.entityfilemanager.helper;

import java.io.File;

import org.brandao.entityfilemanager.SimpleEntityFileAccess;

public class EntityEntityFileAccess 
	extends SimpleEntityFileAccess<Entity, byte[], EntityEntityFileAccessHeader>{

	public EntityEntityFileAccess(String name, File file) {
		super(name, file, new EntityEntityFileAccessHandler());
		this.metadata = new EntityEntityFileAccessHeader();
	}
	
}
