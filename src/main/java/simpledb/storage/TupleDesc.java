package simpledb.storage;

import simpledb.common.FieldType;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    private static final String ANONYMOUS_FIELD_NAME = "anonymous";
    private List<ColumnMeta> columnMetas = new ArrayList<>();
    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<ColumnMeta> iterator() {
        // some code goes here
        return columnMetas.iterator();
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(FieldType[] typeAr, String[] fieldAr) {
        // some code goes here
        for (int i = 0; i < typeAr.length; i++) {
            columnMetas.add(new ColumnMeta(typeAr[i], fieldAr[i]));
        }
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(FieldType[] typeAr) {
        // some code goes here
        for (FieldType fieldType : typeAr) {
            columnMetas.add(new ColumnMeta(fieldType, ANONYMOUS_FIELD_NAME));
        }
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        return columnMetas.size();
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        return columnMetas.get(i).fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public FieldType getFieldType(int i) throws NoSuchElementException {
        return columnMetas.get(i).fieldType;
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        for (int i = 0; i < columnMetas.size(); i++) {
            if (columnMetas.get(i).fieldName.equals(name)) {
                return i;
            }
        }
        throw new NoSuchElementException("no column with name: " + name);
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        int size = 0;
        for (ColumnMeta meta : columnMetas) {
            size += meta.fieldType.getLen();
        }
        return size;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        int totalSize = td1.numFields() + td2.numFields();
        FieldType[] fieldTypes = new FieldType[totalSize];
        String[] fieldNames = new String[totalSize];
        int index = 0;
        index = td1.renderMergeArray(fieldTypes, fieldNames, index);
        td2.renderMergeArray(fieldTypes, fieldNames, index);
        return new TupleDesc(fieldTypes, fieldNames);
    }

    private int renderMergeArray(FieldType[] fieldTypes, String[] fieldNames, int beginIndex) {
        int index = beginIndex;
        for (ColumnMeta meta : columnMetas) {
            fieldTypes[index] = meta.fieldType;
            fieldNames[index] = meta.fieldName;
            index ++;
        }
        return index;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TupleDesc tupleDesc = (TupleDesc) o;
        if (tupleDesc.columnMetas.size() != columnMetas.size()) {
            return false;
        }
        for (int i = 0; i < columnMetas.size(); i++) {
            if (!Objects.equals(columnMetas.get(i), tupleDesc.columnMetas.get(i))) {
                return false;
            }
        }
        return true;
    }

    @Override
    public int hashCode() {
        return Objects.hash(columnMetas);
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
        // some code goes here
        return "";
    }

    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class ColumnMeta implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        public final FieldType fieldType;

        /**
         * The name of the field
         * */
        public final String       fieldName;

        public ColumnMeta(FieldType t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ColumnMeta meta = (ColumnMeta) o;
            return fieldType == meta.fieldType && Objects.equals(fieldName, meta.fieldName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(fieldType, fieldName);
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }
}
