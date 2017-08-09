package com.infomaximum.rocksdb.builder;

import com.infomaximum.rocksdb.core.struct.DomainObject;
import com.infomaximum.rocksdb.struct.RocksDataBase;
import com.infomaximum.rocksdb.utils.TypeConvertRocksdb;
import org.rocksdb.*;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

/**
 * Created by kris on 07.10.16.
 */
public class RocksdbBuilder {

	private DBOptions dbOptions;
	private Path path;

	private Set<Class<? extends DomainObject>> maintenanceClasses = new HashSet<>();

	public RocksdbBuilder() {
		RocksDB.loadLibrary();

		this.dbOptions = new DBOptions();
		this.dbOptions.setCreateIfMissing(true);
	}

	public RocksdbBuilder withPath(String path) {
		this.path = Paths.get(path);
		return this;
	}

	public RocksdbBuilder withPath(Path path) {
		this.path = path;
		return this;
	}

	public RocksdbBuilder addMaintenanceClass(Class<? extends DomainObject> clazz) {
		maintenanceClasses.add(clazz);
		return this;
	}

	public RocksDataBase build() throws RocksDBException {
		//Загружаем список columnFamilyName
		Options options = new Options().setCreateIfMissing(true);
		List<ColumnFamilyDescriptor> columnFamilyDescriptors = new ArrayList<ColumnFamilyDescriptor>();
		for (byte[] columnFamilyName: RocksDB.listColumnFamilies(options, path.toAbsolutePath().toString())) {
			columnFamilyDescriptors.add(new ColumnFamilyDescriptor(columnFamilyName));
		}
		if (columnFamilyDescriptors.isEmpty()) columnFamilyDescriptors.add(new ColumnFamilyDescriptor("default".getBytes(TypeConvertRocksdb.ROCKSDB_CHARSET)));
		options.close();


		//Подключаемся к базе данных
		List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<ColumnFamilyHandle>();
		RocksDB rocksDB = RocksDB.open(dbOptions, path.toAbsolutePath().toString(), columnFamilyDescriptors, columnFamilyHandles);

		Map<String, ColumnFamilyHandle> columnFamilies = new HashMap<String, ColumnFamilyHandle>();
		for (int i=0; i<columnFamilyDescriptors.size(); i++) {
			String columnFamilyName = new String(columnFamilyDescriptors.get(i).columnFamilyName(), TypeConvertRocksdb.ROCKSDB_CHARSET);
			ColumnFamilyHandle columnFamilyHandle = columnFamilyHandles.get(i);
			columnFamilies.put(columnFamilyName, columnFamilyHandle);
		}


		//Запускаем миграционные скрипты
//		if (migrationItems==null) throw new RuntimeException("Not found migration engine");
//		MigrationWorker migrationWorker = new MigrationWorker(
//				rocksDB, migrationItems
//		);
//		migrationWorker.work();

		//Теперь можно создавать соединение с базой данной
		return new RocksDataBase(rocksDB, dbOptions, columnFamilies, maintenanceClasses);
	}

}
