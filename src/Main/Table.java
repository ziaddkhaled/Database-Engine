package Main;

import BTree.BTree;

import java.io.*;
import java.util.*;

public class Table implements Serializable {

    public ArrayList<String> PageNames;
    int pageCounter = 0;
    String TableName;
    String key;
    public static HashMap<String, BTree> indices;
    Hashtable<String, String> attr;

    ArrayList<Object[]> MinMaxvalue;

    public Table() {

    }

    public Table(String TableName, String key, Hashtable<String, String> attr) {
        this.TableName = TableName;
        this.key = key;
        this.PageNames = new ArrayList<>();
        this.attr = attr;
        indices = new HashMap<>();
        MinMaxvalue = new ArrayList<>();


    }

    public List<Object[]> getMinMaxvalue() {
        return MinMaxvalue;
    }

    public void setMinMaxvalue(Page page) {
        MinMaxvalue.add(page.getMinMax());
    }

    public String getTableName() {
        return this.TableName;
    }

    public void setTableName(String tableName) {
        TableName = tableName;
    }

    public ArrayList<String> getPagesnames() {
        return PageNames;
    }

    public int PageNumber(Object value, String type) {
        int lastIndex = PageNames.size() - 1;

        // Typecasting the 'value' parameter according to the given type
        Comparable targetValue;
        switch (type) {
            case "java.lang.Integer":
                targetValue = (Integer) value;
                break;
            case "java.lang.Double":
                targetValue = (Double) value;
                break;
            case "java.lang.String":
                targetValue = (String) value;
                break;
            // Add more cases for other types if needed
            default:
                throw new IllegalArgumentException("Unsupported type: " + type);
        }
        int low = 0;
        int high = lastIndex;

        while (low <= high) {
            int mid = low + (high - low) / 2;

            // Typecasting the minMaxVal according to the given type
            Comparable minValue;
            Comparable maxValue;
            switch (type) {
                case "java.lang.Integer":
                    minValue = (Integer) MinMaxvalue.get(mid)[0];
                    maxValue = (Integer) MinMaxvalue.get(mid)[1];
                    break;
                case "java.lang.Double":
                    minValue = (Double) MinMaxvalue.get(mid)[0];
                    maxValue = (Double) MinMaxvalue.get(mid)[1];
                    break;
                case "java.lang.String":
                    minValue = (String) MinMaxvalue.get(mid)[0];
                    maxValue = (String) MinMaxvalue.get(mid)[1];
                    break;
                // Add more cases for other types if needed
                default:
                    throw new IllegalArgumentException("Unsupported type: " + type);
            }

            int comparemin = targetValue.compareTo(minValue);
            int comparemax = targetValue.compareTo((maxValue));
            if (comparemax <= 0 && comparemin >= 0) {
                return mid;
            } else if (comparemin < 0) {
                high = mid - 1;
            } else {
                low = mid + 1;
            }
        }

        return PageNames.size() - 1;
    }

    public int PageNumberforupdate(String value, String type) {
        int lastIndex = PageNames.size() - 1;
        int index = 0;

        // Typecasting the 'value' parameter according to the given type
        Comparable targetValue;
        switch (type) {
            case "java.lang.Integer":
                targetValue = Integer.parseInt(value);
                break;
            case "java.lang.Double":
                targetValue = Double.parseDouble(value);
                break;
            case "java.lang.String":
                targetValue = (String) value;
                break;
            // Add more cases for other types if needed
            default:
                throw new IllegalArgumentException("Unsupported type: " + type);
        }

        int low = 0;
        int high = lastIndex;

        while (low <= high) {
            int mid = low + (high - low) / 2;

            // Typecasting the minMaxVal according to the given type
            Comparable minValue;
            Comparable maxValue;
            switch (type) {
                case "java.lang.Integer":
                    minValue = (Integer) MinMaxvalue.get(mid)[0];
                    maxValue = (Integer) MinMaxvalue.get(mid)[1];
                    break;
                case "java.lang.Double":
                    minValue = (Double) MinMaxvalue.get(mid)[0];
                    maxValue = (Double) MinMaxvalue.get(mid)[1];
                    break;
                case "java.lang.String":
                    minValue = (String) MinMaxvalue.get(mid)[0];
                    maxValue = (String) MinMaxvalue.get(mid)[1];
                    break;
                // Add more cases for other types if needed
                default:
                    throw new IllegalArgumentException("Unsupported type: " + type);
            }

            int comparemax = targetValue.compareTo(minValue);
            int comparemin = targetValue.compareTo((maxValue));
            if (comparemax <= 0 && comparemin >= 0) {
                return mid;
            } else if (comparemin < 0) {
                high = mid - 1;
            } else {
                low = mid + 1;
            }
        }

        return PageNames.size() - 1;
    }


    public int addTupleToPage(Tuple tuple) {
        // Check if the tuple contains a primary key
        if (!tuple.ContainPrimaryKey())
            return -2;

        // If there are no pages yet, create a new page and add the tuple
        if (PageNames.isEmpty()) {
            pageCounter++;
            Page p = new Page(TableName + pageCounter);
            p.AddTuple(tuple, key);
            MinMaxvalue.add(p.getMinMax());
            PageNames.add(TableName + pageCounter);
            p.serializePage();
            return 0;
        }

        int index = PageNumber(tuple.getValue(key), attr.get(key));
        int tempi = index;
        String name = PageNames.get(index);
        Page currentPage = Page.deserializePage(name);

        // Check if the tuple already exists in the current page
        if (currentPage.getTuple_BS(tuple.getPrimaryKeyValue(), key) != null)
            return -1;

        Tuple lastTuple = tuple;
        Tuple currentTuple = tuple;
        // If the current page is not full, simply add the tuple to it
        if (!currentPage.isFull()) {
            currentPage.AddTuple(tuple, key);
            currentPage.serializePage();
            MinMaxvalue.set(index,currentPage.getMinMax());
            return index;
        } else {
            while (tempi < PageNames.size() - 1) {
                // Load the next page
                Page nextPage = Page.deserializePage(PageNames.get(tempi + 1));
                // Remove the last tuple from the current page
                lastTuple = currentPage.removelast();
                // Add the current tuple to the current page
                currentPage.AddTuple(currentTuple, key);
                MinMaxvalue.set(tempi, currentPage.getMinMax());
                // If the next page is not full, add the last tuple to it and return
                if (!nextPage.isFull()) {
                    currentPage.serializePage();
                    nextPage.AddTuple(lastTuple, key);
                    MinMaxvalue.set(tempi, nextPage.getMinMax());
                    nextPage.serializePage();
                    return index;
                }
                // Serialize the current page and move to the next one
                currentPage.serializePage();
                currentPage = nextPage;
                currentTuple = lastTuple;
                tempi++;
            }
            Page.deserializePage(currentPage.fileName);

            // Check if the last page is full and create a new page if needed
            if (tempi == PageNames.size() - 1 && currentPage.isFull()) {
                if (((Comparable) currentPage.getMinMax()[1]).compareTo(currentTuple.getPrimaryKeyValue()) < 0) {
                    pageCounter++;
                    Page newPage = new Page(TableName + pageCounter);
                    newPage.AddTuple(lastTuple, key);
                    MinMaxvalue.add(newPage.getMinMax());
                    PageNames.add(TableName + pageCounter);
                    MinMaxvalue.add(newPage.getMinMax());
                    newPage.serializePage();
                    return index;
                } else {
                    // Remove the last tuple from the current page
                    lastTuple = currentPage.removelast();
                    // Add the current tuple to the current page
                    currentPage.AddTuple(currentTuple, key);
                    MinMaxvalue.set(tempi, currentPage.getMinMax());
                    currentPage.serializePage();
                }

                // Create a new page for the last tuple
                pageCounter++;
                Page newPage = new Page(TableName + pageCounter);
                newPage.AddTuple(lastTuple, key);
                MinMaxvalue.add(newPage.getMinMax());
                PageNames.add(TableName + pageCounter);
                MinMaxvalue.add(newPage.getMinMax());
                newPage.serializePage();
                return index;
            }
        }
        return index;
    }


    public final void writeObject(ObjectOutputStream out) throws DBAppException, IOException {
        out.defaultWriteObject();
        out.writeObject(PageNames);
    }

    public final void readObject(ObjectInputStream in) throws DBAppException, ClassNotFoundException, IOException {
        in.defaultReadObject();
        PageNames = (ArrayList<String>) in.readObject();
    }

    public Set<String> getColumns() {
        return attr.keySet();
    }

    public void serializeTable() {
        try {
            FileOutputStream fileOut = new FileOutputStream(TableName);
            ObjectOutputStream out = new ObjectOutputStream(fileOut);
            out.writeObject(this);
            out.close();
            fileOut.close();
            //System.out.printf("Serialized data is saved in %s%n", TableName);
        } catch (IOException i) {
            i.printStackTrace();
        }
    }

    public static Table deserializeTable(String fileName) {
        Table table = null;
        try {
            FileInputStream fileIn = new FileInputStream(fileName);
            ObjectInputStream in = new ObjectInputStream(fileIn);
            table = (Table) in.readObject();
            table.setTableName(fileName);
            in.close();
            fileIn.close();
            //System.out.printf("deserialized data is saved in %s%n", fileName);
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
        return table;
    }


    public Page getPageAtPosition(int i) {
        System.out.println(
                Page.deserializePage(PageNames.get(i)).getTuples()
        );
        return Page.deserializePage(PageNames.get(i));
    }

    public boolean isEmpty() {
        return getPagesnames().isEmpty();
    }

    public int getSize() {
        int sum = 0;
        for (String s : PageNames) {
            Page page = Page.deserializePage(s);
            sum += page.Tuples.size();
        }
        return sum;
    }

    public ArrayList<BTree> getBTrees() {
        ArrayList<BTree> t = new ArrayList<>();
        for (Map.Entry<String, BTree> entry : indices.entrySet()) {
            t.add(entry.getValue());
        }
        return t;
    }

    public HashMap<String, BTree> getIndices() {
        return indices;
    }

    public boolean tableContainsColumn(String columnName) {
        return attr.containsKey(columnName);
    }

    public Class<?> getColumnType(String columnName) {
        String type = attr.get(columnName);
        Class<?> cls;

        switch (type) {
            case "java.lang.Integer":
                cls = Integer.class;

                break;
            case "java.lang.Double":
                cls = Double.class;
                break;

            case "java.lang.String":
                cls = String.class;
                break;
            // Add more cases for other types if needed
            default:
                throw new IllegalArgumentException("Unsupported type: " + type);
        }
        return cls;

    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (String s : PageNames) {
            Page page = Page.deserializePage(s);
            sb.append(page).append("\n");
        }
        return sb.toString();
    }
}
