package org.brandao.entityfilemanager;

import java.io.File;

import org.brandao.entityfilemanager.LongEntityFileAccessHandler.LongEntityFileAccessHeader;

public class LongEntityFileAccess 
	extends SimpleEntityFileAccess<Long, byte[], LongEntityFileAccessHeader>{

	public LongEntityFileAccess(File file) {
		super(file, new LongEntityFileAccessHandler());
		this.metadata = new LongEntityFileAccessHeader();
	}

}
