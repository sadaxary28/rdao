package com.infomaximum.rocksdb.migration;

import com.infomaximum.rocksdb.migration.struct.IMigrationItem;
import org.rocksdb.RocksDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Created by kris on 21.07.16.
 */
public class MigrationWorker {

    private final static Logger log = LoggerFactory.getLogger(MigrationWorker.class);

    private final RocksDB rocksDB;
    private final List<IMigrationItem> migrationItems;

    public MigrationWorker(RocksDB rocksDB, List<IMigrationItem> migrationItems) {
        this.rocksDB = rocksDB;
        this.migrationItems = migrationItems;

        //валидируем миграционные скрипты
//        validationMigrationItems();
    }

    /** Запускаем upgrade */
    public void work() throws Exception {
//        String url = configuration.getProperty("hibernate.connection.url");
//
//        //Проверяем наличие базы данных и валидность uuid
//        try (Connection connection = DriverManager.getConnection(url)) {
//            connection.setAutoCommit (false);
//            if (!isExistTableService(connection)) {
//                createTableService(connection);
//                saveDatabaseUuid(connection, subSystem.getUuid());
//                saveDatabaseVersion(connection, subSystem.getVersion());
//                connection.commit();
//                return;
//            } else {
//                String databaseUuid = getDatabaseUuid(connection);
//                if (!subSystem.getUuid().equals(databaseUuid)) throw new RuntimeException("Invalid database uuid: " + databaseUuid);
//            }
//        }
//
//        //Крутимся пока полность не обновимсь
//        while (upgradeNextMigration(url));
    }

//    private boolean upgradeNextMigration(String url) throws Exception {

//        try (Connection connection = DriverManager.getConnection(url)) {
//            connection.setAutoCommit (false);
//
//            String versionDatabase = getDatabaseVersion(connection);
//            if (versionDatabase!=null && VersionUtils.isEquals(versionDatabase, subSystem.getVersion())) return false;
//
//            log.info("Need upgrade database: version: {}", versionDatabase);
//
//            IMigrationItem nextMigrationItem = getMigration(migrationItems, versionDatabase);
//
//            log.info("Upgrade from {} to {}...", versionDatabase, nextMigrationItem.getCompletedVersion());
//
//            if (typeDialect == TypeDialect.SQLITE) {
//                nextMigrationItem.migrationFromSQLite(connection);
//            } else {
//                throw new RuntimeException("Not support type dialect: " + typeDialect);
//            }
//            saveDatabaseVersion(connection, nextMigrationItem.getCompletedVersion());
//
//            connection.commit();
//
//            log.info("Upgrade from {} to {}... completed", versionDatabase, nextMigrationItem.getCompletedVersion());
//
//            return true;
//        } catch (Exception e) {
//            log.error("Error migration worker: upgradeNextMigration", e);
//            throw e;
//        }
//    }

//    private boolean isExistTableService(Connection connection){
//        try (PreparedStatement ps = connection.prepareStatement("SELECT * FROM service")) {
//            ps.executeQuery();
//            return true;
//        } catch (Exception e) {
//            return false;
//        }
//    }

//    private void createTableService(Connection connection) throws SQLException {
//        String sql;
//        if (typeDialect==TypeDialect.SQLITE) {
//            sql = "CREATE TABLE service (key varchar(255) not null unique, version bigint not null, value TEXT, primary key (key))";
//        } else if (typeDialect==TypeDialect.HSQLDB) {
//            sql = "CREATE TABLE service (key varchar(255) not null, version bigint not null, value LONGVARCHAR, primary key (key))";
//        } else {
//            throw new RuntimeException("Not support dialect: " + typeDialect);
//        }
//        try (PreparedStatement ps = connection.prepareStatement(sql)) {
//            ps.executeUpdate();
//        }
//    }

//    private void saveDatabaseUuid(Connection connection, String uuid) throws Exception {
//        setCryptConfig(connection, KEY_UUID, uuid);
//    }
//
//    private String getDatabaseUuid(Connection connection) throws NoSuchPaddingException, BadPaddingException, NoSuchAlgorithmException, IllegalBlockSizeException, SQLException, InvalidKeyException {
//        return getCryptConfig(connection, KEY_UUID);
//    }
//
//    private String getDatabaseVersion(Connection connection) throws NoSuchPaddingException, BadPaddingException, NoSuchAlgorithmException, IllegalBlockSizeException, SQLException, InvalidKeyException {
//        return getCryptConfig(connection, KEY_VERSION);
//    }
//
//    private void saveDatabaseVersion(Connection connection, String version) throws Exception {
//        setCryptConfig(connection, KEY_VERSION, version);
//    }
//
//    private String getCryptConfig(Connection connection, String key) throws SQLException, IllegalBlockSizeException, NoSuchPaddingException, BadPaddingException, NoSuchAlgorithmException, InvalidKeyException {
//        try (PreparedStatement ps = connection.prepareStatement("SELECT value FROM service WHERE key = ?")) {
//            ps.setString(1, key);
//            ResultSet rs = ps.executeQuery();
//            if (!rs.next()) return null;
//            return CryptConfig.decrypt(rs.getString("value"));
//        }
//    }
//
//    private void setCryptConfig(Connection connection, String key, String value) throws NoSuchPaddingException, BadPaddingException, NoSuchAlgorithmException, IllegalBlockSizeException, SQLException, InvalidKeyException {
//        String oldValue = getCryptConfig(connection, key);
//        String cryptValue = CryptConfig.encrypt(value);
//        if (oldValue==null) {
//            try (PreparedStatement ps = connection.prepareStatement("INSERT INTO service (key, version, value) VALUES (?, 0, ?);")) {
//                ps.setString(1, key);
//                ps.setString(2, cryptValue);
//                ps.executeUpdate();
//            }
//        } else {
//            try (PreparedStatement ps = connection.prepareStatement("UPDATE service SET value=?, version=version+1 WHERE key=?;")) {
//                ps.setString(1, cryptValue);
//                ps.setString(2, key);
//                ps.executeUpdate();
//            }
//        }
//    }
//
//    private void validationMigrationItems() {
//        Set<IMigrationItem> poolItems = new HashSet<IMigrationItem>(migrationItems);
//
//        String chainVersion=null;
//        while (poolItems.size()>0) {
//            IMigrationItem item = getMigration(poolItems, chainVersion);
//            if (item==null) new RuntimeException("Not valid migration item, not found: " + chainVersion);
//
//            poolItems.remove(item);
//            chainVersion = item.getCompletedVersion();
//        }
//
//        if (!subSystem.getVersion().equals(chainVersion)) throw new RuntimeException("Final chain version is not actual");
//    }
//
//    private static IMigrationItem getMigration(Collection<IMigrationItem> items, String version) {
//        for (IMigrationItem item: items) {
//            if (version == null || item.getMigrationVersion() == null) {
//                if (version == null && item.getMigrationVersion() == null) return item;
//            } else {
//                if (VersionUtils.isEquals(item.getMigrationVersion(), version)) return item;
//            }
//        }
//        throw new RuntimeException("Not found migration item from version: " + version);
//    }
}
