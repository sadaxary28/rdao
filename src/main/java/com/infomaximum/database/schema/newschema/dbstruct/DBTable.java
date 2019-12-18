package com.infomaximum.database.schema.newschema.dbstruct;

import com.infomaximum.database.exception.FieldNotFoundException;
import com.infomaximum.database.exception.SchemaException;
import com.infomaximum.database.schema.newschema.StructEntity;
import net.minidev.json.JSONObject;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class DBTable extends DBObject {

    public static class Reference {

        public final int tableId;
        public final int hashIndexId;

        Reference(int tableId, int hashIndexId) {
            this.tableId = tableId;
            this.hashIndexId = hashIndexId;
        }
    }

    private static final String JSON_PROP_NAME = "name";
    private static final String JSON_PROP_NAMESPACE = "namespace";
    private static final String JSON_PROP_FIELDS = "fields";
    private static final String JSON_PROP_HASH_INDEXES = "hash_indexes";
    private static final String JSON_PROP_PREFIX_INDEXES = "prefix_indexes";
    private static final String JSON_PROP_INTERVAL_INDEXES = "interval_indexes";
    private static final String JSON_PROP_RANGE_INDEXES = "range_indexes";

    private final String dataColumnFamily;
    private final String indexColumnFamily;

    private String name;
    private final String namespace;
    private final List<DBField> fields;
    private final List<DBHashIndex> hashIndexes;
    private final List<DBPrefixIndex> prefixIndexes;
    private final List<DBIntervalIndex> intervalIndexes;

    private final List<Reference> referencingForeignFields = new ArrayList<>();

    private DBTable(int id, String name, String namespace, List<DBField> fields,
                    List<DBHashIndex> hashIndexes, List<DBPrefixIndex> prefixIndexes,
                    List<DBIntervalIndex> intervalIndexes) {
        super(id);
        this.dataColumnFamily = namespace + StructEntity.NAMESPACE_SEPARATOR + name;
        this.indexColumnFamily = namespace + StructEntity.NAMESPACE_SEPARATOR + name + ".index";
        this.name = name;
        this.namespace = namespace;
        this.fields = fields;
        this.hashIndexes = hashIndexes;
        this.prefixIndexes = prefixIndexes;
        this.intervalIndexes = intervalIndexes;
    }

    DBTable(int id, String name, String namespace, List<DBField> fields) {
        this(id, name, namespace, fields, new ArrayList<>(), new ArrayList<>(), new ArrayList<>());
    }

    public String getDataColumnFamily() {
        return dataColumnFamily;
    }

    public String getIndexColumnFamily() {
        return indexColumnFamily;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getNamespace() {
        return namespace;
    }

    public List<DBField> getFields() {
        return fields;
    }

    public List<Reference> getReferencingForeignFields() {
        return referencingForeignFields;
    }

    public DBField newField(String name, Class<? extends Serializable> type, Integer foreignTableId) {
        DBField field = new DBField(DBSchema.nextId(fields), name, type, foreignTableId);
        fields.add(field);
        return field;
    }

    public int findFieldIndex(String fieldName) {
        for (int i = 0; i < fields.size(); ++i) {
            if (fields.get(i).getName().equals(fieldName)) {
                return i;
            }
        }
        return -1;
    }

    public int getFieldIndex(String fieldName) throws SchemaException {
        int i = findFieldIndex(fieldName);
        if (i == -1) {
            throw new FieldNotFoundException(fieldName, getName());
        }
        return i;
    }

    public DBField getField(String fieldName) throws SchemaException {
        return fields.get(getFieldIndex(fieldName));
    }

    public List<DBHashIndex> getHashIndexes() {
        return hashIndexes;
    }

    public List<DBPrefixIndex> getPrefixIndexes() {
        return prefixIndexes;
    }

    public List<DBIntervalIndex> getIntervalIndexes() {
        return intervalIndexes;
    }

    public Stream<? extends DBIndex> getIndexesStream() {
        return Stream.concat(Stream.concat(
                hashIndexes.stream(),
                prefixIndexes.stream()),
                intervalIndexes.stream());
    }

    public void attachIndex(DBHashIndex index) {
        attachIndex(index, hashIndexes);
    }

    public void attachIndex(DBPrefixIndex index) {
        attachIndex(index, prefixIndexes);
    }

    public void attachIndex(DBIntervalIndex index) {
        attachIndex(index, intervalIndexes);
    }

    void checkIntegrity() throws SchemaException {
        DBSchema.checkUniqueId(fields);
        DBSchema.checkUniqueId(getIndexesStream().collect(Collectors.toList()));

        Set<Integer> indexingFieldIds = new HashSet<>(fields.size());

        for (DBHashIndex i : hashIndexes) {
            IntStream.of(i.getFieldIds()).forEach(indexingFieldIds::add);
        }

        for (DBPrefixIndex i : prefixIndexes) {
            IntStream.of(i.getFieldIds()).forEach(indexingFieldIds::add);
        }

        for (DBIntervalIndex i : intervalIndexes) {
            indexingFieldIds.add(i.getIndexedFieldId());
            IntStream.of(i.getHashFieldIds()).forEach(indexingFieldIds::add);
        }

        Set<Integer> existingFieldIds = fields.stream().map(DBObject::getId).collect(Collectors.toSet());
        indexingFieldIds.stream()
                .filter(fieldId -> !existingFieldIds.contains(fieldId))
                .findFirst()
                .ifPresent(fieldId -> {
                    throw new SchemaException("Field id=" + fieldId + " not found into '" + getName() + "'");
                });
    }

    private <T extends DBIndex> void attachIndex(T index, List<T> destination) {
        index.setId(DBSchema.nextId(getIndexesStream()));
        destination.add(index);
    }

    static DBTable fromJson(JSONObject source) throws SchemaException {
        return new DBTable(
                JsonUtils.getValue(JSON_PROP_ID, Integer.class, source),
                JsonUtils.getValue(JSON_PROP_NAME, String.class, source),
                JsonUtils.getValue(JSON_PROP_NAMESPACE, String.class, source),
                JsonUtils.toList(JSON_PROP_FIELDS, source, DBField::fromJson),
                JsonUtils.toList(JSON_PROP_HASH_INDEXES, source, DBHashIndex::fromJson),
                JsonUtils.toList(JSON_PROP_PREFIX_INDEXES, source, DBPrefixIndex::fromJson),
                JsonUtils.toList(JSON_PROP_INTERVAL_INDEXES, source, DBIntervalIndex::fromJson)
        );
    }

    @Override
    JSONObject toJson() {
        JSONObject object = new JSONObject();
        object.put(JSON_PROP_ID, getId());
        object.put(JSON_PROP_NAME, name);
        object.put(JSON_PROP_NAMESPACE, namespace);
        object.put(JSON_PROP_FIELDS, JsonUtils.toJsonArray(fields));
        object.put(JSON_PROP_HASH_INDEXES, JsonUtils.toJsonArray(hashIndexes));
        object.put(JSON_PROP_PREFIX_INDEXES, JsonUtils.toJsonArray(prefixIndexes));
        object.put(JSON_PROP_INTERVAL_INDEXES, JsonUtils.toJsonArray(intervalIndexes));
        return object;
    }
}
