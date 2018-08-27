package org.brandao.entityfilemanager.tx;

import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.concurrent.locks.ReentrantLock;

import org.brandao.entityfilemanager.AbstractEntityFileAccess;
import org.brandao.entityfilemanager.EntityFileAccess;

public class TransactionEntityFileAccess<T, R, H> 
	extends AbstractEntityFileAccess<TransactionalEntity<T>, RawTransactionEntity<R>, TransactionHeader<H>> {

	protected EntityFileAccess<T, R, H> parent;
	
	protected EntityFileTransactionDataHandler<T,R,H> transactionDataHandler;
	
	public TransactionEntityFileAccess(EntityFileAccess<T, R, H> e, File file, long transactionID, 
			byte transactionIsolation){
		super(
				e.getName(), 
				file, 
				new EntityFileTransactionDataHandler<T,R,H>(e.getEntityFileDataHandler())
		);
		
		this.parent = e;
		this.transactionDataHandler = (EntityFileTransactionDataHandler<T,R,H>)super.dataHandler;
		this.metadata = new TransactionHeader<H>(e.getMetadata());
	}

	public void setTransactionStatus(byte value) throws IOException{
		this.fileAccess.seek(this.transactionDataHandler.getTransactionStatusPointer());
		this.fileAccess.writeByte(value);
		this.metadata.setTransactionStatus(value);
	}
	
	public byte getTransactionStatus() throws IOException{
		return metadata.getTransactionStatus();
	}
	
	public boolean isStarted(){
		return metadata.getTransactionStatus() != EntityFileTransaction.TRANSACTION_NOT_STARTED;
	}

	public long getTransactionID() throws IOException{
		return metadata.getTransactionID();
	}

	public void setTransactionID(long value) throws IOException {
		this.fileAccess.seek(this.transactionDataHandler.getTransactionIDPointer());
		this.fileAccess.writeLong(value);
		this.metadata.setTransactionID(value);;
	}

	public void setTransactionIsolation(byte value) throws IOException{
		this.fileAccess.seek(this.transactionDataHandler.getTransactionIsolationPointer());
		this.fileAccess.writeByte(value);
		this.metadata.setTransactionIsolation(value);
	}
	
	public byte getTransactionIsolation() throws IOException{
		return metadata.getTransactionIsolation();
	}
	
	public EntityFileAccess<T, R, H> getEntityFileAccess() {
		return parent;
	}
	
	private void writeObject(ObjectOutputStream stream) throws IOException {
		stream.writeObject(metadata);
		stream.writeUTF(file.getName());
		stream.writeInt(batchLength);
		stream.writeObject(dataHandler);
		stream.writeObject(parent);
    }

    @SuppressWarnings("unchecked")
	private void readObject(ObjectInputStream stream) throws IOException, ClassNotFoundException {
    	metadata               = (TransactionHeader<H>) stream.readObject();
		file                   = new File(stream.readUTF());
		batchLength            = stream.readInt();
		dataHandler            = (EntityFileTransactionDataHandler<T,R,H>) stream.readObject();
		parent                 = (EntityFileAccess<T, R, H>)stream.readObject();
		
		transactionDataHandler = (EntityFileTransactionDataHandler<T,R,H>)dataHandler;
		
		this.lock              = new ReentrantLock();
		
		this.open();
    }
	
}
