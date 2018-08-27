package org.brandao.entityfilemanager.tx;

import java.io.IOException;
import java.io.RandomAccessFile;

import org.brandao.entityfilemanager.AbstractEntityFileAccess;
import org.brandao.entityfilemanager.DataInputStream;
import org.brandao.entityfilemanager.DataOutputStream;
import org.brandao.entityfilemanager.EntityFileAccess;
import org.brandao.entityfilemanager.EntityFileDataHandler;
import org.brandao.entityfilemanager.FileAccess;

public class SubtransactionEntityFileAccess<T, R, H> 
	extends AbstractEntityFileAccess<T, R, H>{

	private long maxLength;
	
	public SubtransactionEntityFileAccess(long pointer, long maxLength, 
			RandomAccessFile transactionFile, EntityFileAccess<T, R, H> efa) {
		super(efa.getName(), efa.getAbsoluteFile(), 
				new DataHandler<T, R, H>(pointer, efa.getEntityFileDataHandler()));
		this.maxLength    = maxLength;
		this.fileAccess   = new FileAccess(file, transactionFile);
		this.firstPointer = pointer;
	}

	public void createNewFile() throws IOException {
	}

	public void open() throws IOException {
	}
	
	public void setLength(long value){
		throw new UnsupportedOperationException();
	}

	public void delete() throws IOException {
	}
	
	private static class DataHandler<T, R, H> 
		implements EntityFileDataHandler<T, R, H>{

		private EntityFileDataHandler<T, R, H> dataHandler;
		
		private long pointer;
		
		public DataHandler(long pointer, EntityFileDataHandler<T, R, H> dataHandler){
			this.pointer = pointer;
			this.dataHandler = dataHandler;
		}
		
		public void writeMetaData(DataOutputStream stream, H value)
				throws IOException {
			dataHandler.writeMetaData(stream, value);
		}

		public H readMetaData(DataInputStream srteam) throws IOException {
			return dataHandler.readMetaData(srteam);
		}

		public void writeEOF(DataOutputStream stream) throws IOException {
			dataHandler.writeEOF(stream);
		}

		public void write(DataOutputStream stream, T entity) throws IOException {
			dataHandler.write(stream, entity);
		}

		public void writeRaw(DataOutputStream stream, R entity)
				throws IOException {
			dataHandler.writeRaw(stream, entity);
		}

		public T read(DataInputStream stream) throws IOException {
			return dataHandler.read(stream);
		}

		public R readRaw(DataInputStream stream) throws IOException {
			return dataHandler.readRaw(stream);
		}

		public int getRecordLength() {
			return dataHandler.getRecordLength();
		}

		public int getEOFLength() {
			return dataHandler.getEOFLength();
		}

		public long getFirstRecord() {
			return pointer + dataHandler.getFirstRecord();
		}

		public Class<T> getType() {
			return dataHandler.getType();
		}

		public Class<R> getRawType() {
			return dataHandler.getRawType();
		}

		public int getHeaderLength() {
			return dataHandler.getHeaderLength();
		}
		
	}
}
