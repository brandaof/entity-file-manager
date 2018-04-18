package org.brandao.entityfilemanager;

import java.io.IOException;

public class DataUtil {

	public static final String DEFAULT_ENCODE = "UTF-8";
	
	public static final byte[] stringToBytes(String value, int bufferLength) throws IOException{
		byte[] buffer = new byte[bufferLength];

		if(value != null){
			byte[] data   = value.getBytes(DEFAULT_ENCODE);
			int size      = data.length;
			int dataSize  = size + 2;
			
			if(size > Short.MAX_VALUE)
				throw new IOException(size + " > " + Short.MAX_VALUE);
	
			if(dataSize > bufferLength)
				throw new IOException(dataSize + " > " + bufferLength);
			
			buffer[0] = (byte)(size & 0xff);
			buffer[1] = (byte)((size >> 8) & 0xff);
			
			System.arraycopy(data, 0, buffer, 2, data.length);
		}
		else{
			short size = -1;
			buffer[0]  = (byte)(size & 0xff);
			buffer[1]  = (byte)((size >> 8) & 0xff);
			
		}
		return buffer;
	}

	public static final byte[] stringsToBytes(String[] value, int bufferLength) throws IOException{
		byte[] buffer = new byte[bufferLength];
		buffer[0] = (byte)(value.length & 0xff);
		buffer[1] = (byte)((value.length >> 8) & 0xff);
		
		int start = 2;
		for(String item: value){
			byte[] data = stringToBytes(item, item.length() + 2);
			
			if(start + data.length > buffer.length )
				throw new IOException((start + data.length) + " > " + buffer.length);
			
			System.arraycopy(data, 0, buffer, start, data.length);
			start += data.length;
		}
		return buffer;
	}
	
	public static final String bytesToString(byte[] value, int start) throws IOException{
		return bytesToString(value, DEFAULT_ENCODE, start);
	}
	
	public static final String bytesToString(byte[] value, String encode, int start) throws IOException{
		
		short size = (short)(
				 value[start    ]       & 0xffL |
				(value[start + 1] << 8  & 0xff00)
		);

		if(size == -1)
			return null;
		
		int init = start + 2;
		int end  = init + size;
		
		if(end > value.length)
			throw new IOException(end + " > " + value.length);

		return new String(value, init, size, encode);
	}

	public static final String[] bytesToStrings(byte[] value, int start) throws IOException{
		return bytesToStrings(value, DEFAULT_ENCODE, start);
	}
	
	public static final String[] bytesToStrings(byte[] value, String encode, int start) throws IOException{
		
		short length = (short)(
				 value[start    ]       & 0xffL |
				(value[start + 1] << 8  & 0xff00)
		);
		
		String[] result = new String[length];
		start += 2;
		for(int i=0;i<length;i++){
			result[i] = bytesToString(value, encode, start);
			start += result[i].length() + 2;
		}
		
		return result;
	}
	
}
