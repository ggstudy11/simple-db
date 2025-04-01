package simpledb.storage;

import simpledb.common.Type;

import java.io.Serializable;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.List;
import java.util.ArrayList;


/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    /**
     * tdItems 集合
     */
    private final List<TDItem> tdItems;

    public List<TDItem> getTdItems() {
        return tdItems;
    }

    /**
     * A help class to facilitate organizing the information of each field
     */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         */
        public final Type fieldType;

        /**
         * The name of the field
         */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    /**
     * @return An iterator which iterates over all the field TDItems
     *         that are included in this TupleDesc
     */
    public Iterator<TDItem> iterator() {
        return tdItems.iterator();
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     *
     * @param typeAr  array specifying the number of and types of fields in this
     *                TupleDesc. It must contain at least one entry.
     * @param fieldAr array specifying the names of the fields. Note that names may
     *                be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        int typeArLen = typeAr.length;
        this.tdItems = new ArrayList<>(typeArLen);
        for (int i = 0; i < typeArLen; i++) {
            tdItems.add(new TDItem(typeAr[i], fieldAr[i]));
        }
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     *
     * @param typeAr array specifying the number of and types of fields in this
     *               TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        int typeArLen = typeAr.length;
        this.tdItems = new ArrayList<>(typeArLen);
        for (Type type : typeAr) {
            tdItems.add(new TDItem(type, null));
        }
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        return tdItems.size();
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     *
     * @param i index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        if (i < 0 || i >= numFields()) {
            throw new NoSuchElementException("Not a valid index!");
        }
        return tdItems.get(i).fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     *
     * @param i The index of the field to get the type of. It must be a valid
     *          index.
     * @return the type of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        if (i < 0 || i >= numFields()) {
            throw new NoSuchElementException("Not a valid index!");
        }
        return tdItems.get(i).fieldType;
    }

    /**
     * Find the index of the field with a given name.
     *
     * @param name name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException if no field with a matching name is found.
     */
    public int indexForFieldName(String name) throws NoSuchElementException {
        for (int i = 0; i < numFields(); i++) {
            String fieldName = tdItems.get(i).fieldName;
            if (fieldName == null) continue;
            if (fieldName.equals(name)) return i;
        }
        throw new NoSuchElementException("No field with a matching name is found!");
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        int size = 0;
        for (TDItem item : tdItems) {
            size += item.fieldType.getLen();
        }
        return size;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     *
     * @param td1 The TupleDesc with the first fields of the new TupleDesc
     * @param td2 The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        int newLen = td1.numFields() + td2.numFields();
        Type[] typeAr = new Type[newLen];
        String[] fieldAr = new String[newLen];
        for (int i = 0; i < td1.numFields(); i++) {
            typeAr[i] = td1.getTdItems().get(i).fieldType;
            fieldAr[i] = td1.getTdItems().get(i).fieldName;
        }
        for (int i = 0; i < td2.numFields(); i++) {
            typeAr[i + td1.numFields()] = td2.getTdItems().get(i).fieldType;
            fieldAr[i + td1.numFields()] = td2.getTdItems().get(i).fieldName;
        }
        return new TupleDesc(typeAr, fieldAr);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they have the same number of items
     * and if the i-th type in this TupleDesc is equal to the i-th type in o
     * for every i.
     *
     * @param o the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */

    public boolean equals(Object o) {
        // o instanceof TupleDesc
        if (this == o) {
            return true;
        }
        if (o instanceof TupleDesc == false) {
            return false;
        }
        TupleDesc td = (TupleDesc) o;
        if (numFields() != td.numFields() || getSize() != td.getSize()) {
            return false;
        }
        for (int i = 0; i < numFields(); i++) {
            if (!getFieldType(i).equals(td.getFieldType(i))) {
                return false;
            }
        }
        return true;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     *
     * @return String describing this descriptor.
     */
    public String toString() {

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < numFields(); i++) {
            sb.append(tdItems.get(i).toString() + ",");
        }
        // 删除最后一个,
        sb.deleteCharAt(sb.length() - 1);
        return sb.toString();
    }
}
