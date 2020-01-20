package com.infomaximum.database.schema.dbstruct;

import com.infomaximum.database.exception.FieldNotFoundException;
import com.infomaximum.database.exception.runtime.SchemaException;
import com.infomaximum.database.schema.StructEntity;
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
    private final List<DBField> sortedFields;
    private final List<DBHashIndex> hashIndexes;
    private final List<DBPrefixIndex> prefixIndexes;
    private final List<DBIntervalIndex> intervalIndexes;
    private final List<DBRangeIndex> rangeIndexes;

    private final List<Reference> referencingForeignFields = new ArrayList<>();

    private DBTable(int id, String name, String namespace, List<DBField> sortedFields,
                    List<DBHashIndex> hashIndexes, List<DBPrefixIndex> prefixIndexes,
                    List<DBIntervalIndex> intervalIndexes, List<DBRangeIndex> rangeIndexes) {
        super(id);
        this.dataColumnFamily = namespace + StructEntity.NAMESPACE_SEPARATOR + name;
        this.indexColumnFamily = namespace + StructEntity.NAMESPACE_SEPARATOR + name + ".index";
        this.name = name;
        this.namespace = namespace;
        this.sortedFields = sortedFields;
        this.hashIndexes = hashIndexes;
        this.prefixIndexes = prefixIndexes;
        this.intervalIndexes = intervalIndexes;
        this.rangeIndexes = rangeIndexes;
    }

    DBTable(int id, String name, String namespace, List<DBField> sortedFields) {
        this(id, name, namespace, sortedFields, new ArrayList<>(), new ArrayList<>(), new ArrayList<>(), new ArrayList<>());
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

    public List<DBField> getSortedFields() {
        return sortedFields;
    }

    public List<Reference> getReferencingForeignFields() {
        return referencingForeignFields;
    }

    public DBField newField(String name, Class<? extends Serializable> type, Integer foreignTableId) {
        DBField field = new DBField(DBSchema.nextId(sortedFields), name, type, foreignTableId);
        sortedFields.add(field);
        return field;
    }

    public int findFieldIndex(String fieldName) {
        for (int i = 0; i < sortedFields.size(); ++i) {
            if (sortedFields.get(i).getName().equals(fieldName)) {
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
        return sortedFields.get(getFieldIndex(fieldName));
    }

    public DBField getField(int id) throws SchemaException {
        return sortedFields.get(id);
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

    public List<DBRangeIndex> getRangeIndexes() {
        return rangeIndexes;
    }

    public Stream<? extends DBIndex> getIndexesStream() {
        return Stream.concat(Stream.concat(Stream.concat(
                hashIndexes.stream(),
                prefixIndexes.stream()),
                intervalIndexes.stream()),
                rangeIndexes.stream());
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

    public void attachIndex(DBRangeIndex index) {
        attachIndex(index, rangeIndexes);
    }

    void checkIntegrity() throws SchemaException {
        DBSchema.checkUniqueId(sortedFields);
        DBSchema.checkUniqueId(getIndexesStream().collect(Collectors.toList()));
        checkFieldsOrder();

        Set<Integer> indexingFieldIds = new HashSet<>(sortedFields.size());

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

        for (DBRangeIndex i : rangeIndexes) {
            indexingFieldIds.add(i.getBeginFieldId());
            indexingFieldIds.add(i.getEndFieldId());
            IntStream.of(i.getHashFieldIds()).forEach(indexingFieldIds::add);
        }

        Set<Integer> existingFieldIds = sortedFields.stream().map(DBObject::getId).collect(Collectors.toSet());
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

    private void checkFieldsOrder() {
        int id = 0;
        for (DBField sortedField : sortedFields) {
            if (sortedField.getId() != id) {
                throw new SchemaException("Table " + namespace + "." + name + " has inconsistent fields order: " + sortedFields.stream()
                        .map(DBObject::getId)
                        .collect(Collectors.toList()));
            }
        }
    }

    static DBTable fromJson(JSONObject source) throws SchemaException {
        List<DBField> fields = JsonUtils.toList(JSON_PROP_FIELDS, source, DBField::fromJson);
        return new DBTable(
                JsonUtils.getValue(JSON_PROP_ID, Integer.class, source),
                JsonUtils.getValue(JSON_PROP_NAME, String.class, source),
                JsonUtils.getValue(JSON_PROP_NAMESPACE, String.class, source),
                fields,
                JsonUtils.toList(JSON_PROP_HASH_INDEXES, source, s -> DBHashIndex.fromJson(s, fields)),
                JsonUtils.toList(JSON_PROP_PREFIX_INDEXES, source, s ->  DBPrefixIndex.fromJson(s, fields)),
                JsonUtils.toList(JSON_PROP_INTERVAL_INDEXES, source, s ->  DBIntervalIndex.fromJson(s, fields)),
                JsonUtils.toList(JSON_PROP_RANGE_INDEXES, source, s ->  DBRangeIndex.fromJson(s, fields))
        );
    }

    @Override
    JSONObject toJson() {
        JSONObject object = new JSONObject();
        object.put(JSON_PROP_ID, getId());
        object.put(JSON_PROP_NAME, name);
        object.put(JSON_PROP_NAMESPACE, namespace);
        object.put(JSON_PROP_FIELDS, JsonUtils.toJsonArray(sortedFields));
        object.put(JSON_PROP_HASH_INDEXES, JsonUtils.toJsonArray(hashIndexes));
        object.put(JSON_PROP_PREFIX_INDEXES, JsonUtils.toJsonArray(prefixIndexes));
        object.put(JSON_PROP_INTERVAL_INDEXES, JsonUtils.toJsonArray(intervalIndexes));
        object.put(JSON_PROP_RANGE_INDEXES, JsonUtils.toJsonArray(rangeIndexes));
        return object;
    }
}
