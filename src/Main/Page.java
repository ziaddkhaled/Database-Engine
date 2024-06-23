package Main;

import java.io.*;
import java.util.Comparator;
import java.util.Vector;

public class Page implements Serializable {
    Vector<Tuple> Tuples;
    String fileName;
    final int MaxSize=DBApp.size;
    transient boolean IsFull;
    public Page(){
        Tuples = new Vector<>();

    }
    public Page(String fileName){
        this.fileName = fileName;
        Tuples = new Vector<>();
    }

    public final void writeObject(ObjectOutputStream out) throws DBAppException, IOException {
        out.defaultWriteObject();
        out.writeObject(Tuples);
    }

    public void setTuples(Vector<Tuple> t){
        this.Tuples=t;}

    public static void deletePage(String pageFileName) {
        try {
            File file = new File(pageFileName);

            // Check if the file exists
            if (file.exists()) {
                // Attempt to delete the file
                if (file.delete()) {
                    System.out.println("Page " + pageFileName + " deleted successfully.");
                } else {
                    System.out.println("Failed to delete page " + pageFileName);
                }
            } else {
                System.out.println("Page " + pageFileName + " does not exist.");
            }
        } catch (Exception e) {
            System.out.println("An error occurred while deleting the page: " + e.getMessage());
        }
    }



    public final void readObject(ObjectInputStream in) throws DBAppException, ClassNotFoundException, IOException {
        in.defaultReadObject();
        Tuples = (Vector<Tuple>) in.readObject();
    }
    public Tuple firstelement(){
        if(!Tuples.isEmpty())
            return Tuples.get(0);
        return null;
    }
    public Tuple lastelement(){
        if(!Tuples.isEmpty())
          return Tuples.get(getTuples().size()-1);
        return null;
    }

    public Object[] getMinMax(){
        return new Object[]{Tuples.get(0).getPrimaryKeyValue(),Tuples.get(Tuples.size()-1).getPrimaryKeyValue()};
    }
    public Tuple getTuple_BS(Object clusteringKeyValue, String clusteringKey) {
        int low = 0;
        int high = Tuples.size() - 1;

        while (low <= high) {
            int mid = low + (high - low) / 2;
            Tuple midTuple = Tuples.get(mid);
            Object midValue = midTuple.getValue(clusteringKey);

            int cmp = ((Comparable) midValue).compareTo(clusteringKeyValue);

            if (cmp == 0) {
                return midTuple;
            }
            else if (cmp > 0) {
                high = mid - 1;
            }

            else {
                low = mid + 1;
            }
        }

        return null;
    }

    public Tuple getTuple_BSforupdate(String clusteringKeyValue, String clusteringKey,String type) {
        int low = 0;
        int high = Tuples.size() - 1;


        while (low <= high) {
            int mid = low + (high - low) / 2;
            Tuple midTuple = Tuples.get(mid);
            Object midValue = midTuple.getValue(clusteringKey);
            Comparable c;
            switch (type) {
                case "java.lang.Integer":
                    c = Integer.parseInt(clusteringKeyValue);
                    break;
                case "java.lang.Double":
                    c = Double.parseDouble(clusteringKeyValue);
                    break;
                case "java.lang.String":
                    c = (String) clusteringKeyValue;
                    break;
                // Add more cases for other types if needed
                default:
                    throw new IllegalArgumentException("Unsupported type: " + type);
            }


            int cmp = ((Comparable) midValue).compareTo(c);

            if (cmp == 0) {
                return midTuple;
            }
            else if (cmp > 0) {
                high = mid - 1;
            }

            else {
                low = mid + 1;
            }
        }

        return null;
    }




    public boolean AddTuple(Tuple tuple, String key){
        if(getTuples().size()<MaxSize){
            Object val = (int) tuple.getValue(key);
            Tuples.add(tuple);
            Tuples.sort(Comparator.comparingInt(t -> (int) t.getValue(key)));
            //System.out.println(tuple+"successfully added");
        }
        return true;
    }
    public Vector<Tuple> getTuples(){
        return Tuples;
    }
    public Tuple removelast(){
        return Tuples.remove(Tuples.size()-1);
    }
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();
        for (Tuple tuple : Tuples) {
            stringBuilder.append(tuple.toString()).append(",");
        }
        return stringBuilder.toString();
    }

    public void serializePage() {
        try {
            FileOutputStream fileOut = new FileOutputStream(fileName);
            ObjectOutputStream out = new ObjectOutputStream(fileOut);
            out.writeObject(this);
            out.close();
            fileOut.close();
            //System.out.printf("Serialized data is saved in %s%n", fileName);
        } catch (IOException i) {
            i.printStackTrace();
        }
    }



    public static Page deserializePage(String fileName) {
        Page page = null;
        try {
            FileInputStream fileIn = new FileInputStream(fileName);
            ObjectInputStream in = new ObjectInputStream(fileIn);
            page = (Page) in.readObject();
            in.close();
            fileIn.close();
            //System.out.printf("deserialized data is saved in %s%n", fileName);
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
        return page;
    }


    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public String getFileName() {
        return fileName;
    }

    public int getSize() {
        return Tuples.size();
    }

    public boolean isFull() {
        return Tuples.size()>=MaxSize;
    }

    public void setMinMax() {
        if (Tuples.isEmpty()) {
            return;
        }
        Object min = Tuples.firstElement().getPrimaryKeyValue();
        Object max = Tuples.lastElement().getPrimaryKeyValue();
    }
}

