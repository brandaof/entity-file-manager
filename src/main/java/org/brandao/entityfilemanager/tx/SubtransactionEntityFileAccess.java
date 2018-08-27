package org.brandao.entityfilemanager.tx;

import java.io.IOException;
import java.io.RandomAccessFile;

import org.brandao.entityfilemanager.AbstractEntityFileAccess;
import org.brandao.entityfilemanager.DataInputStream;
import org.brandao.entityfilemanager.DataOutputStream;
import org.brandao.entityfilemanager.EntityFileAccess;
import org.brandao.entityfilemanager.EntityFileDataHandler;
import org.brandao.entityfilemanager.FileAccess;
import org.brandao.entityfilemanager.tx.SubtransactionEntityFileAccess.*;

public class SubtransactionEntityFileAccess<T, R, H> 
	extends AbstractEntityFileAccess<T, R, SubtransactionHeader<H>>{

	public SubtransactionEntityFileAccess(
			long pointer, RandomAccessFile transactionFile, 
			EntityFileAccess<T, R, H> efa) throws IOException {
		
		super(efa.getName(), efa.getAbsoluteFile(), 
				new DataHandler<T, R, H>(pointer, efa.getEntityFileDataHandler()));
		
		this.metadata     = new SubtransactionHeader<H>(efa.length(), efa.getMetadata());
		this.fileAccess   = new FileAccess(this.file, transactionFile);
	}

	public void createNewFile() throws IOException {
		this.fileAccess.seek(this.dataHandler.getFirstPointer());
		this.writeHeader();
		this.setLength(0);
	}

	public void open() throws IOException {
		this.fileAccess.seek(this.dataHandler.getFirstPointer());
		this.readHeader();
		
		long maxPointer =
				this.dataHandler.getFirstPointer() +
				this.dataHandler.getFirstRecord() + 
				this.dataHandler.getRecordLength()*this.metadata.getMaxLength();
		
		long currentMaxPointer = this.fileAccess.length();
		
		this.length = 
				currentMaxPointer > maxPointer? 
						this.metadata.getMaxLength() : 
						(currentMaxPointer - this.dataHandler.getFirstPointer()) / this.dataHandler.getRecordLength(); 
	}
	
	public void setLength(long value) throws IOException{
		
		if(length > this.metadata.getMaxLength()){
			throw new IOException(value + " > " + this.metadata.getMaxLength());
		}
		
		this.length = value;
	}

	public void delete() throws IOException {
	}
	
	public static class SubtransactionHeader<H>{
		
		private long maxLength;

		private H parent;
		
		public SubtransactionHeader(long maxLength, H parent){
			this.parent = parent;
			this.maxLength = maxLength;
		}
		public long getMaxLength() {
			return maxLength;
		}

		public void setMaxLength(long maxLength) {
			this.maxLength = maxLength;
		}
		
		public H getParent() {
			return parent;
		}
		
		public void setParent(H parent) {
			this.parent = parent;
		}
		
	}
	
	private static class DataHandler<T, R, H> 
		implements EntityFileDataHandler<T, R, SubtransactionHeader<H>>{

		private EntityFileDataHandler<T, R, H> dataHandler;
		
		private long firstPointer;
		
		public DataHandler(long firstPointer, EntityFileDataHandler<T, R, H> dataHandler){
			this.dataHandler = dataHandler;
		}
		
		public void writeMetaData(DataOutputStream stream, SubtransactionHeader<H> value)
				throws IOException {
			dataHandler.writeMetaData(stream, value.getParent());
			stream.writeLong(value.getMaxLength());
		}

		public SubtransactionHeader<H> readMetaData(DataInputStream stream) throws IOException {
			H parent       = dataHandler.readMetaData(stream);
			long maxLength = stream.readLong();
			return new SubtransactionHeader<H>(maxLength, parent);
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
			return dataHandler.getFirstRecord() + 8;
		}

		public Class<T> getType() {
			return dataHandler.getType();
		}

		public Class<R> getRawType() {
			return dataHandler.getRawType();
		}

		public int getHeaderLength() {
			return this.getFirstRecord();
		}

		public long getFirstPointer() {
			return this.firstPointer;
		}
		
	}
}
