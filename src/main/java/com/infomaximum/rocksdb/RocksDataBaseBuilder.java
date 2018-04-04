package com.infomaximum.rocksdb;

import com.infomaximum.database.exception.DatabaseException;
import com.infomaximum.database.utils.PathUtils;
import com.infomaximum.database.utils.TypeConvert;
import com.infomaximum.util.TempLibraryCleaner;
import org.rocksdb.*;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class RocksDataBaseBuilder {

    private Path path;

    public RocksDataBaseBuilder withPath(Path path) {
        this.path = path.toAbsolutePath();
        return this;
    }

    public RocksDBProvider build() throws DatabaseException {
        TempLibraryCleaner.clear();
        PathUtils.checkPath(path);
        try (Options options = buildOptions()) {
            List<ColumnFamilyDescriptor> columnFamilyDescriptors = getColumnFamilyDescriptors(options);

            List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();
            OptimisticTransactionDB rocksDB = OptimisticTransactionDB.open(options, path.toString(), columnFamilyDescriptors, columnFamilyHandles);

            ConcurrentMap<String, ColumnFamilyHandle> columnFamilies = new ConcurrentHashMap<>();
            for (int i = 0; i < columnFamilyDescriptors.size(); i++) {
                String columnFamilyName = TypeConvert.unpackString(columnFamilyDescriptors.get(i).columnFamilyName());
                ColumnFamilyHandle columnFamilyHandle = columnFamilyHandles.get(i);
                columnFamilies.put(columnFamilyName, columnFamilyHandle);
            }

            return new RocksDBProvider(rocksDB, columnFamilies);
        } catch (RocksDBException e) {
            throw new DatabaseException(e);
        }
    }

    private Options buildOptions() throws RocksDBException {
        final String optionsFilePath = path.toString() + ".ini";

        Options options;
        if (Paths.get(optionsFilePath).toFile().exists()) {
            options = RocksDB.loadOptionsFromFile(optionsFilePath, false);
        } else {
            options = new Options().
                    setInfoLogLevel(InfoLogLevel.WARN_LEVEL).
                    setMaxTotalWalSize(100L * 1024L * 1024L);
        }

        return options.setCreateIfMissing(true);
    }

    private List<ColumnFamilyDescriptor> getColumnFamilyDescriptors(Options options) throws RocksDBException {
        List<ColumnFamilyDescriptor> columnFamilyDescriptors = new ArrayList<>();

        for (byte[] columnFamilyName : RocksDB.listColumnFamilies(options, path.toString())) {
            columnFamilyDescriptors.add(new ColumnFamilyDescriptor(columnFamilyName));
        }
        if (columnFamilyDescriptors.isEmpty()) {
            columnFamilyDescriptors.add(new ColumnFamilyDescriptor(TypeConvert.pack(RocksDBProvider.DEFAULT_COLUMN_FAMILY)));
        }

        return columnFamilyDescriptors;
    }
}
