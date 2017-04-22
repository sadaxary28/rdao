package com.infomaximum.rocksdb.builder;

import com.infomaximum.rocksdb.migration.struct.IMigrationItem;
import com.infomaximum.rocksdb.struct.RocksDataBase;
import org.rocksdb.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by kris on 07.10.16.
 */
public class RocksdbBuilder {

	private DBOptions dbOptions;
	private String path;

	private List<IMigrationItem> migrationItems;

	public RocksdbBuilder() {
		this.dbOptions = new DBOptions();
		this.dbOptions.setCreateIfMissing(true);
	}

	public RocksdbBuilder withPath(String path) {
		this.path=path;
		return this;
	}

	public RocksDataBase build() throws Exception {
		RocksDB.loadLibrary();

		//Загружаем список columnFamilyName
		Options options = new Options().setCreateIfMissing(true);
		List<ColumnFamilyDescriptor> columnFamilyDescriptors = new ArrayList<ColumnFamilyDescriptor>();
		for (byte[] columnFamilyName: RocksDB.listColumnFamilies(options, path)) {
			columnFamilyDescriptors.add(new ColumnFamilyDescriptor(columnFamilyName));
		}
		if (columnFamilyDescriptors.isEmpty()) columnFamilyDescriptors.add(new ColumnFamilyDescriptor("default".getBytes()));
		options.close();


		//Подключаемся к базе данных
		List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<ColumnFamilyHandle>();
		RocksDB rocksDB = RocksDB.open(dbOptions, path, columnFamilyDescriptors, columnFamilyHandles);

		Map<String, ColumnFamilyHandle> columnFamilies = new HashMap<String, ColumnFamilyHandle>();
		for (int i=0; i<columnFamilyDescriptors.size(); i++) {
			String columnFamilyName = new String(columnFamilyDescriptors.get(i).columnFamilyName());
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
		return new RocksDataBase(rocksDB, dbOptions, columnFamilies);
	}

}
