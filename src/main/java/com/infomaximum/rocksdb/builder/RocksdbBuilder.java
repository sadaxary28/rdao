package com.infomaximum.rocksdb.builder;

import com.infomaximum.database.domainobject.DomainObject;
import com.infomaximum.database.utils.TypeConvert;
import com.infomaximum.rocksdb.struct.RocksDataBase;
import org.rocksdb.*;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Created by kris on 07.10.16.
 */
public class RocksdbBuilder {

	private Path path;

	public RocksdbBuilder withPath(String path) {
		this.path = Paths.get(path);
		return this;
	}

	public RocksdbBuilder withPath(Path path) {
		this.path = path;
		return this;
	}

	public RocksDataBase build() throws RocksDBException {
		//Загружаем список columnFamilyName
		List<ColumnFamilyDescriptor> columnFamilyDescriptors = new ArrayList<>();
		try (Options options = new Options().setCreateIfMissing(true)) {
			for (byte[] columnFamilyName : RocksDB.listColumnFamilies(options, path.toAbsolutePath().toString())) {
				columnFamilyDescriptors.add(new ColumnFamilyDescriptor(columnFamilyName));
			}
			if (columnFamilyDescriptors.isEmpty()) {
				columnFamilyDescriptors.add(new ColumnFamilyDescriptor(TypeConvert.pack(RocksDataBase.DEFAULT_COLUMN_FAMILY)));
			}
		}

        //Подключаемся к базе данных
        List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();
        RocksDB rocksDB;
        try (DBOptions dbOptions = new DBOptions().setCreateIfMissing(true)) {
            rocksDB = RocksDB.open(dbOptions, path.toAbsolutePath().toString(), columnFamilyDescriptors, columnFamilyHandles);
        }

        ConcurrentMap<String, ColumnFamilyHandle> columnFamilies = new ConcurrentHashMap<>();
        for (int i = 0; i < columnFamilyDescriptors.size(); i++) {
            String columnFamilyName = TypeConvert.getString(columnFamilyDescriptors.get(i).columnFamilyName());
            ColumnFamilyHandle columnFamilyHandle = columnFamilyHandles.get(i);
            columnFamilies.put(columnFamilyName, columnFamilyHandle);
        }

        //Теперь можно создавать соединение с базой данной
        return new RocksDataBase(rocksDB, columnFamilies);
	}
}
