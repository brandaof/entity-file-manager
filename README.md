# Entity file manager
Permite registrar entidades em arquivos estruturados que possuem suporte transacional.

Como usar:

1.  Criar a implementação do EntityFileDataHandler;
2.  Criar o header do arquivo;
3.  Instanciar alguma implementação do EntityFileAccess;
4.  Iniciar o EntityFileManager e registrar o EntityFileAccess.


Exemplo:

### Entity

```
public class Entity {

	private int id;
	
	private int status;
	
	private String message;

	...
	
}
```

### EntityFileDataHandler

```
public class EntityEntityFileAccessHandler 
	implements EntityFileDataHandler<Entity, byte[], EntityEntityFileAccessHeader>{

	private static final int ROW_LENGTH = 151;

	private static final int DATA_LENGTH = 150;
	
	private static final int FIRST_POINTER = 0;
	
	private static final int HEADER_LENGTH = 0;
	
	private static final int EOF_LENGTH = 1;
	
	private static final int FIRST_RECORD = HEADER_LENGTH;
	
	private byte[] emptyEntity;
	
	private byte[] readBuffer;
	
	public EntityEntityFileAccessHandler(){
		emptyEntity = new byte[DATA_LENGTH];
		readBuffer  = new byte[DATA_LENGTH];
	}
	
	public void writeMetaData(DataWritter stream,
			EntityEntityFileAccessHeader value) throws IOException {
	}

	public EntityEntityFileAccessHeader readMetaData(DataReader srteam)
			throws IOException {
		return new EntityEntityFileAccessHeader();
	}

	public void writeEOF(DataWritter stream) throws IOException {
		stream.writeByte((byte)-1);
	}

	public void write(DataWritter stream, Entity entity) throws IOException {
		stream.writeByte((byte)(entity == null? 0 : 1));
		if(entity == null){
			stream.write(emptyEntity);
		}
		else{
			stream.writeInt(entity.getId());
			stream.writeInt(entity.getStatus());
			stream.writeString(entity.getMessage(), 142);
		}
	}

	public void writeRaw(DataWritter stream, byte[] entity) throws IOException {
		stream.write(entity);
	}

	public Entity read(DataReader stream) throws IOException {
		boolean isnull = stream.readByte() == 0;
		
		if(isnull){
			stream.read(readBuffer);
			return null;
		}
		else{
			Entity e = new Entity();
			e.setId(stream.readInt());
			e.setStatus(stream.readInt());
			e.setMessage(stream.readString(142));
			return e;
		}
		
	}

	public byte[] readRaw(DataReader stream) throws IOException {
		byte[] buffer = new byte[ROW_LENGTH];
		stream.read(buffer);
		return buffer;
	}

	public long getFirstPointer() {
		return FIRST_POINTER;
	}

	public int getHeaderLength() {
		return HEADER_LENGTH;
	}

	public int getRecordLength() {
		return ROW_LENGTH;
	}

	public int getEOFLength() {
		return EOF_LENGTH;
	}

	public int getFirstRecord() {
		return FIRST_RECORD;
	}

	public Class<Entity> getType() {
		return Entity.class;
	}

	public Class<byte[]> getRawType() {
		return byte[].class;
	}

}
```
### Header

```
public static class EntityEntityFileAccessHeader{
}
```

### Instanciando o EntityFileAccess

```
File entityFile = new File("entity");
EntityFileAccess<Entity, byte[], EntityEntityFileAccessHeader> entityEntityFileAccess =
    new SimpleEntityFileAccess<Entity, byte[], EntityEntityFileAccessHeader>(
        "entity", entityFile, new EntityEntityFileAccessHeader())

``` 

### Iniciando o EntityFileManager

```
File path   = new File("./data");
File txPath = new File(path, "tx");

EntityFileManagerConfigurer efm = new EntityFileManagerImp();

LockProvider lp = new LockProviderImp();

EntityFileTransactionManagerConfigurer tm = new EntityFileTransactionManagerImp();

tm.setLockProvider(lp);
tm.setTimeout(EntityFileTransactionManagerImp.DEFAULT_TIMEOUT);
tm.setTransactionPath(txPath);
tm.setEntityFileManagerConfigurer(efm);

efm.setEntityFileTransactionManager(tm);
efm.setLockProvider(lp);
efm.setPath(path);
efm.register("entity", entityEntityFileAccess);
efm.init();


EntityFileTransaction tx = efm.beginTransaction();
try{
    EntityFile<Long> ef = efm.getEntityFile("entity", tx, Entity.class);
    Entity e = new Entity();
    ...
    ef.insert(e);
    tx.commit();
}
catch(Throwable e){
    tx.rollback();
}
```    
