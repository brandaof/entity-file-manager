package org.brandao.entityfilemanager;

import java.io.File;

public class SimpleEntityFileAccess<T, R, H> 
	extends AbstractEntityFileAccess<T, R, H>{

	public SimpleEntityFileAccess(File file,
			EntityFileDataHandler<T, R, H> dataHandler) {
		super(file, dataHandler);
	}

}
