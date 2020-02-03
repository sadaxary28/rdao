package com.infomaximum.database.schema;

import com.infomaximum.database.exception.DatabaseException;
import com.infomaximum.database.provider.DBProvider;
import com.infomaximum.database.schema.dbstruct.DBSchema;
import com.infomaximum.database.utils.RangeIndexUtils;
import com.infomaximum.database.utils.TypeConvert;

import static com.infomaximum.database.schema.Schema.*;

public class TempFix {

    public static DBSchema fixSchemaIfNeed(DBSchema dbSchema, DBProvider dbProvider, RangeIndexUtils.BiConsumer<DBSchema, DBProvider> saveSchema) throws DatabaseException {
        if(dbSchema.getTables().isEmpty() || dbSchema.getTables().stream().anyMatch(t -> t.getId() == 0)) {
            return dbSchema;
        }
        dbSchema.tempFix();
        saveSchema.accept(dbSchema, dbProvider);

        String version = TypeConvert.unpackString(dbProvider.getValue(SERVICE_COLUMN_FAMILY, VERSION_KEY));
        String schemaJson = TypeConvert.unpackString(dbProvider.getValue(SERVICE_COLUMN_FAMILY, SCHEMA_KEY));
        return DBSchema.fromStrings(version, schemaJson);
    }
}
