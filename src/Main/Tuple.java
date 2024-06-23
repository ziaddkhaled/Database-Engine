package Main;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class Tuple implements Serializable,Comparable {
    private Map<String, Object> values;

    private String keyname;

    public Tuple(String keyname) {
        this.keyname = keyname;
        this.values = new HashMap<>();
    }

    public void setValue(String columnName, Object value) {
        values.put(columnName, value);
    }
public boolean ContainPrimaryKey(){
        return values.containsKey(keyname);
}


    public Object getValue(String columnName) {
        return values.get(columnName);
    }

    public String toString() {
        String result = " ";
        for (Map.Entry<String, Object> entry : values.entrySet()) {
            result = result  +entry.getValue() + "-";
        }
        return result;
    }

    public Object getPrimaryKeyValue() {
        return getValue(keyname);
    }

    public Map<String, Object> getValues() {
        return values;
    }

    @Override
    public int compareTo(Object o) {
        Tuple otherTuple = (Tuple) o;

        // Ensure that the object being compared is indeed a Tuple
        if (!(o instanceof Tuple)) {
            throw new IllegalArgumentException("Object must be an instance of Tuple.");
        }

        // Get the primary key values of this tuple and the other tuple
        Comparable<Object> thisPrimaryKey = (Comparable<Object>) this.getValue(keyname);
        Comparable<Object> otherPrimaryKey = (Comparable<Object>) otherTuple.getValue(keyname);

        // Compare the primary key values
        return thisPrimaryKey.compareTo(otherPrimaryKey);
    }
}


