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
	
	public SubtransactionEntityFileAccess(
			long pointer, RandomAccessFile transactionFile, EntityFileAccess<T, R, H> efa) throws IOException {
		super(efa.getName(), efa.getAbsoluteFile(), 
				new DataHandler<T, R, H>(efa.getEntityFileDataHandler()));
		this.maxLength    = efa.length();
		this.fileAccess   = new FileAccess(this.file, transactionFile);
		this.firstPointer = pointer;
		this.metadata     = efa.getMetadata();
	}

	public void createNewFile() throws IOException {
		this.fileAccess.seek(this.firstPointer);
		this.writeHeader();
		this.setLength(0);
	}

	public void open() throws IOException {
		this.fileAccess.seek(this.firstPointer);
		this.readHeader();
		
		long maxPointer =
				this.firstPointer +
				this.dataHandler.getFirstRecord() + 
				this.dataHandler.getRecordLength()*maxLength;
		
		long currentMaxPointer = this.fileAccess.length();
		
		this.length = 
				currentMaxPointer > maxPointer? 
						this.maxLength : 
						(currentMaxPointer - this.firstPointer) / this.dataHandler.getRecordLength(); 
	}
	
	public void setLength(long value) throws IOException{
		
		if(length > this.maxLength){
			throw new IOException(value + " > " + maxLength);
		}
		
		this.length = value;
	}

	public void delete() throws IOException {
	}
	
	private static class DataHandler<T, R, H> 
		implements EntityFileDataHandler<T, R, H>{

		private EntityFileDataHandler<T, R, H> dataHandler;
		
		public DataHandler(EntityFileDataHandler<T, R, H> dataHandler){
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

		public int getFirstRecord() {
			return dataHandler.getFirstRecord();
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
