package Main;
/** * @author Wael Abouelsaadat */

import BTree.BTree;
import BTree.Pointer;

import java.io.*;
import java.util.*;

import static Main.DBAppTest.id;
import static Main.DBAppTest.newTableName;


public class DBApp {
	public static int nodeOrder=4;
	ArrayList<String> TableNames;

	public static int size=property("MaximumRowsCountinPage");




	public DBApp( ){
		//tables = new ArrayList<>();
		TableNames=new ArrayList<>();



	}

	// this does whatever initialization you would like 
	// or leave it empty if there is no code you want to 
	// execute at application startup 
	public void init( ){



	}
	public static int property(String prop){
		Properties properties = new Properties();
		try {
			ClassLoader classloader = Thread.currentThread().getContextClassLoader();
			InputStream fileInputStream = classloader.getResourceAsStream("Main/DBApp.config");
			properties.load(fileInputStream);
			assert fileInputStream != null;
			fileInputStream.close();

			return Integer.parseInt(properties.getProperty(prop));

		} catch (IOException e) {
			e.printStackTrace();
		}
		return 0;
	}



	// following method creates one table only
	// strClusteringKeyColumn is the name of the column that will be the primary
	// key and the clustering column as well. The data type of that column will
	// be passed in htblColNameType
	// htblColNameValue will have the column name as key and the data 
	// type as value
	public void createTable(String strTableName,
							String strClusteringKeyColumn,
							Hashtable<String,String> htblColNameType) throws DBAppException {
		try {
			if(TableNames.contains(strTableName))
				throw new DBAppException("Table name already exists");
			if(!htblColNameType.containsKey(strClusteringKeyColumn))
				throw new DBAppException("Clustering key is invalid");

			PrintWriter metadataWriter;
			metadataWriter = new PrintWriter(new FileWriter("metadata.csv", true));

			for (String columnName : htblColNameType.keySet()) {
				String columnType = htblColNameType.get(columnName);
				if(!Objects.equals(columnType, "java.lang.Integer")
						&& !Objects.equals(columnType, "java.lang.String")
						&& !Objects.equals(columnType, "java.lang.Double"))
				{
					throw new DBAppException("Data type not supported");
				}
				metadataWriter.print(strTableName);
				metadataWriter.print(",");
				metadataWriter.print(columnName + "," + columnType);
				if (columnName.equals(strClusteringKeyColumn)) {
					metadataWriter.print(",true");
				} else {
					metadataWriter.print(",false");
				}

				metadataWriter.println(",NULL,NULL"); // Placeholder for IndexName and IndexType
			}

			metadataWriter.close();
			Table t=new Table(strTableName,strClusteringKeyColumn,htblColNameType);
			String S=strTableName;
			//Tables.add(t);
			TableNames.add(S);

			t.serializeTable();

		} catch (IOException e) {
			throw new DBAppException("Error creating table: " + e.getMessage());
		}
	}

	private boolean isValidColumnType(String columnType) {
		try {
			// Attempt to load the class corresponding to the column type
			Class<?> cls = Class.forName(columnType);


			return cls == Integer.class || cls == String.class || cls == Double.class;
		} catch (ClassNotFoundException e) {
			// Class not found, invalid type
			return false;
		}
	}




	// following method creates a B+tree index 
	public void createIndex(String strTableName, String strColName, String strIndexName) throws DBAppException {
		if(!TableNames.contains(strTableName))
			throw new DBAppException("Table does not exist");

		Table t=Table.deserializeTable(strTableName);
		for(BTree c: t.getBTrees()){
			if(c.getIndexName()==strIndexName)
				throw new DBAppException("The index was already created on one of the columns" );
		}


			boolean tableFound = false;
			for (String table : TableNames) {
				if (table.equals(strTableName)) {
					tableFound = true;
					String csvFile = "metadata.csv"; // Specify the path to your CSV file

					try (BufferedReader br = new BufferedReader(new FileReader(csvFile))) {
						StringBuilder newMetadataContent = new StringBuilder();
						String line;
						while ((line = br.readLine()) != null) {
							String[] parts = line.split(",");
							if (parts[0].equals(strTableName) && parts[1].equals(strColName)) {
								parts[4] = strIndexName; // Update IndexName
								parts[5] = "B+tree"; // Update IndexType
							}
							newMetadataContent.append(String.join(",", parts)).append("\n");
						}

						// Write the updated metadata back to the file
						try (PrintWriter metadataWriter = new PrintWriter(new FileWriter(csvFile))) {
							metadataWriter.print(newMetadataContent);
						}

						BTree tree=new BTree(strIndexName);


						t.indices.put(strTableName+"-"+strColName, tree);
						if(!t.PageNames.isEmpty()) {
							for (String pages : t.PageNames) {
								int page=0;
								Page p = Page.deserializePage(pages);
								for(int i=0;i<p.getTuples().size();i++){
									Comparable c=(Comparable)p.getTuples().get(i).getValue(strColName);
									tree.insert(c,page);
								}
								page++;
								}

							}


					} catch (IOException e) {
						throw new DBAppException("Error reading or updating metadata: " + e.getMessage());
					}
					break;
				}
			}

			if (!tableFound) {
				throw new DBAppException("Main.Table " + strTableName + " not found.");
			}
		}



	// following method inserts one row only. 
	// htblColNameValue must include a value for the primary key
	public void insertIntoTable(String strTableName, Hashtable<String, Object> htblColNameValue) throws DBAppException {

		for (int i = 0; i < TableNames.size(); i++) {
			if (TableNames.get(i).equals(strTableName)) {
				Table t = Table.deserializeTable(TableNames.get(i));
				if(t.attr.size()< htblColNameValue.size())
					throw new DBAppException("Tuple contains columns that aren't in the table");

				for (Map.Entry<String, Object> entry : htblColNameValue.entrySet()) {
					String columnName = entry.getKey();
					Object columnValue = entry.getValue();

					// Check if column exists
					if (!t.tableContainsColumn(columnName)) {
						throw new DBAppException("Column " + columnName + " does not exist in table " + strTableName);
					}

					// Check data type compatibility
					Class<?> expectedType = t.getColumnType(columnName);


					if (columnValue != null && !expectedType.isInstance(columnValue)) {
						throw new DBAppException("Tuple's data type doesn't match the column's data type");
					}
				}
				String k=t.key;
				Tuple row = new Tuple(k);
				int PageNumber;

				for (Map.Entry<String, Object> entry : htblColNameValue.entrySet()) {
					row.setValue(entry.getKey(), entry.getValue());
				}

				if(t.getIndices().containsKey(strTableName + "-"+ t.key)){
					BTree tree=t.getIndices().get(strTableName + "-"+ t.key);
					PageNumber=tree.getPageNumberForInsert(k);

					t.addTupleToPage(row);
				}
				else {
					PageNumber = t.addTupleToPage(row);
				}

				if(PageNumber==-1)
					throw new DBAppException("Primary key already exists");
				if (PageNumber==-2)
					throw new DBAppException("Primary key is not found");


				for (Map.Entry<String, Object> entry : htblColNameValue.entrySet()) {
					BTree tree = t.indices.get(strTableName + "-" + entry.getKey());
					if (tree != null) {

						Comparable columnValue = (Comparable) htblColNameValue.get(entry.getKey());
						tree.insert(columnValue,PageNumber);

						System.out.println(tree);
					}

				}
				t.serializeTable();
				return;
			}
		}
		throw new DBAppException("Table does not exist");
	}


	// following method updates one row only
	// htblColNameValue holds the key and new value 
	// htblColNameValue will not include clustering key as column name
	// strClusteringKeyValue is the value to look for to find the row to update.
	public void updateTable(String strTableName, String strClusteringKeyValue , Hashtable<String, Object> htblColNameValue ) throws DBAppException {
		boolean found = false;
		boolean Tfound =false;
		if(!TableNames.contains(strTableName)){
			throw new DBAppException("Table does not exist");
		}

		for (String table : TableNames) {
			if (table.equals(strTableName)) {
				Table t = Table.deserializeTable(table);
				int pageIndex = t.PageNumberforupdate(strClusteringKeyValue, t.attr.get(t.key));
				Page page = t.getPageAtPosition(pageIndex);
				Tuple tupleToUpdate = page.getTuple_BSforupdate(strClusteringKeyValue, t.key,t.attr.get(t.key));
				for (Map.Entry<String, Object> entry : htblColNameValue.entrySet()) {
					String columnName = entry.getKey();
					if (!t.getColumns().contains(columnName)) {
						String errorMessage = "The Tuple contains come columns that aren't in the table";
						throw new DBAppException(errorMessage);
					}
					Class<?> expectedType = t.getColumnType(columnName);
					Object value = entry.getValue();
					if (!expectedType.isInstance(value)) {
						String errorMessage = "Tuple's data type doesn't match the column's data type";
						System.out.println("Error: " + errorMessage);
						throw new DBAppException(errorMessage);
					}
				}
				if (htblColNameValue.containsKey(t.key))
					throw new DBAppException("The input row wants to change the primary key");
				// If the tuple is found, update its values
				if (tupleToUpdate != null) {
					//Object oldPrimaryKeyValue = tupleToUpdate.getValue(t.key); if (!strClusteringKeyValue.equals(tupleToUpdate.getValue(t.key))) {


					Tuple old=new Tuple(t.key);
					old.setValue("id",tupleToUpdate.getValue("id"));
					old.setValue("gpa",tupleToUpdate.getValue("gpa"));
					old.setValue("name",tupleToUpdate.getValue("name"));

					for (String columnName : htblColNameValue.keySet()) {

						tupleToUpdate.setValue(columnName, htblColNameValue.get(columnName));

					}


					for(int j=0;j<t.getIndices().size();j++) {
						System.out.println("entered");

						BTree tree = t.getBTrees().get(j);
						System.out.println(tupleToUpdate);

						if(t.getIndices().containsKey(t.TableName+"-"+"id")){
							tree.delete((Comparable) old.getValue("id"));
							tree.insert((Comparable) tupleToUpdate.getValue("id"),pageIndex);
						}
						if(t.getIndices().containsKey(t.TableName+"-"+"name")){
							System.out.println(old);
							tree.delete((Comparable) old.getValue("name"));
							tree.insert((Comparable) tupleToUpdate.getValue("name"),pageIndex);}
						if(t.getIndices().containsKey(t.TableName+"-"+"gpa")){
							tree.delete((Comparable) old.getValue("gpa"));
							tree.insert((Comparable) tupleToUpdate.getValue("gpa"),pageIndex);
						}

					}

					page.serializePage(); // Serialize the updated page back to disk
					found = true;
					break;
				}
			}
		}
		if (!found) {
			throw new DBAppException("Tuple with clustering key " + strClusteringKeyValue + " not found in table " + strTableName);
		}
	}


	// following method could be used to delete one or more rows.
	// htblColNameValue holds the key and value. This will be used in search 
	// to identify which rows/tuples to delete. 	
	// htblColNameValue enteries are ANDED together
	public void deleteFromTable(String strTableName, Hashtable<String, Object> htblColNameValue) throws DBAppException {
		boolean isTableFound = false;

		for (int i = 0; i < TableNames.size(); i++) {
			if (TableNames.get(i).equals(strTableName)) {
				isTableFound = true;
				Table table = Table.deserializeTable(TableNames.get(i));
				ArrayList<String> tablePagesNames = table.getPagesnames();

				ArrayList<String> pagesToRemove = new ArrayList<>();

				// Extract column names from htblColNameValue
				Set<String> colNamesSet = htblColNameValue.keySet();

				for (String pageName : tablePagesNames) {
					System.out.println("Processing page: " + pageName);
					Page page = Page.deserializePage(pageName);
					Vector<Tuple> tuples = page.getTuples();

					// Get the min and max of the page before deleting
					Object[] minMaxBeforeDelete = page.getMinMax();

					System.out.println("Min value of page before deletion: " + minMaxBeforeDelete[0]);
					System.out.println("Max value of page before deletion: " + minMaxBeforeDelete[1]);

					ArrayList<Tuple> result = new ArrayList<>();
					for (Map.Entry<String, Object> entry : htblColNameValue.entrySet()) {
						String columnName = entry.getKey();
						System.out.println("Checking column: " + columnName);
						// Check if the column exists in the table schema
						if (!table.getColumns().contains(columnName)) {
							String errorMessage = "The Tuple contains come columns that aren't in the table";
							System.out.println("Error: " + errorMessage);
							throw new DBAppException(errorMessage);
						}

						// Get the expected data type for the column
						Class<?> expectedType = table.getColumnType(columnName);

						// Get the provided value
						Object value = entry.getValue();

						// Check if the provided value matches the expected data type
						if (!expectedType.isInstance(value)) {
							String errorMessage = "Tuple's data type doesn't match the column's data type";
							System.out.println("Error: " + errorMessage);
							throw new DBAppException(errorMessage);
						}

						ArrayList<Tuple> temp = new ArrayList<>();
						for (Tuple tuple : tuples) {
							// Check if the tuple contains the column and value to delete
							if (tuple.getValues().containsKey(columnName) && tuple.getValue(columnName).equals(value)) {
								temp.add(tuple);
							}
						}
						if (result.isEmpty()) {
							result.addAll(temp);
						} else {
							result.retainAll(temp);
						}
					}

					System.out.println("Tuples to delete: " + result);

					// Check if the deleted tuples were either the min or max of the page
					boolean minDeleted = false;
					boolean maxDeleted = false;

					// Iterate through the deleted tuples and check if they were the min or max
					for (Tuple deletedTuple : result) {
						Object primaryKeyValue = deletedTuple.getPrimaryKeyValue();
						if (primaryKeyValue.equals(minMaxBeforeDelete[0])) {
							minDeleted = true;
						}
						if (primaryKeyValue.equals(minMaxBeforeDelete[1])) {
							maxDeleted = true;
						}
					}

					// Delete the tuples from the page
					for (Iterator<Tuple> iterator = tuples.iterator(); iterator.hasNext();) {
						Tuple tuple = iterator.next();
						if (result.contains(tuple)) {
							System.out.println("Deleting tuple: " + tuple);
							for (int j = 0; j < table.getIndices().size(); j++) {
								System.out.println("entered");
								BTree t = table.getBTrees().get(j);
								if (table.getIndices().containsKey(table.TableName + "-" + "id"))
									t.delete((Comparable) tuple.getValue("id"));
								if (table.getIndices().containsKey(table.TableName + "-" + "gpa")) {
									t.delete((Comparable) tuple.getValue("gpa"));
								}
								if (table.getIndices().containsKey(table.TableName + "-" + "name"))
									t.delete((Comparable) tuple.getValue("name"));
							}
							iterator.remove();
						}
					}

					// If the min or max of the page was deleted, update min or max accordingly
					if (minDeleted || maxDeleted) {
						page.setMinMax(); // Recalculate min and max
					}

					System.out.println("Remaining tuples on page: " + tuples);

					// Set the page's tuples attribute to the updated ArrayList
					page.setTuples(tuples);
					page.serializePage();

					// If the page is now empty, add it to the list for removal
					if (tuples.isEmpty()) {
						System.out.println("Page is empty: " + pageName);
						pagesToRemove.add(pageName);
						// Remove the page from disk
						Page.deletePage(pageName);
					} else {
						// Serialize the updated page
						System.out.println("Serializing page: " + pageName);
						page.serializePage();
					}
				}

				// Remove empty pages
				tablePagesNames.removeAll(pagesToRemove);

				// Serialize the updated table
				table.serializeTable();

				// Check if the table is empty
				boolean isEmpty = tablePagesNames.isEmpty();
				if (isEmpty) {
					System.out.println("Table is empty.");
				} else {
					System.out.println("Table is not empty.");
				}
			}
		}

		if (!isTableFound) {
			String errorMessage = "Table does not exist";
			System.out.println("Error: " + errorMessage);
			throw new DBAppException(errorMessage);
		}
	}

	public Iterator selectFromTable(SQLTerm[] arrSQLTerms,
									String[]  strarrOperators) throws DBAppException{
		if(arrSQLTerms.length == 0 ){
			throw new DBAppException("no SQL term entered");
		}
		if(arrSQLTerms.length -1 != strarrOperators.length){
			throw new DBAppException("Num of operators must be = SQLTerms -1");
		}
		for(int i=0; i< strarrOperators.length; i++){
			if((strarrOperators[i] != "AND") && (strarrOperators[i] != "OR") && (strarrOperators[i] != "XOR") ){
				throw new DBAppException("The only supported array operators are AND,OR,XOR");
			}
		}
		for(SQLTerm s1 : arrSQLTerms) {
			String table = s1._strTableName;
			String column = s1._strColumnName;
			String operator = s1._strOperator;
			Object value = s1._objValue;
			if (column == null || table == null || operator == null || value == null) {
				throw new DBAppException("invalid SQLTerm");
			}
			if ((operator != "<") && (operator != ">") && (operator != "=") && (operator != "!=") && (operator != ">=") && (operator != "<=")) {
				throw new DBAppException("The only supported operators are <,<=,>,>=,!=,=");
			}
			if (!((value instanceof Integer) || (value instanceof String) || (value instanceof Double))) {
				throw new DBAppException("type of value not supported");
			}
		}
		int i=0;
		ArrayList<Tuple> result= new ArrayList<>();
		for(SQLTerm s1: arrSQLTerms){
			if(i ==0){
				ArrayList<Tuple> t = getSQLTuples(s1);
				result.addAll(t);
			}
			else {
				ArrayList<Tuple> t = getSQLTuples(s1);
				result = getFinalResultSet(result, t, strarrOperators[i-1]);
			}
			i++;

		}
		Iterator iterator = result.iterator();
		return iterator;
	}

	public ArrayList<Tuple> getSQLTuples(SQLTerm s) throws DBAppException {
		String table = s._strTableName;
		String column = s._strColumnName;
		String operator = s._strOperator;
		Object value = s._objValue;
		boolean check = false;
		for(int i=0; i< TableNames.size(); i++){
			if(table == TableNames.get(i)){
				check = true;
			}
		}
		if(!check){
			throw new DBAppException("table not existing");
		}
		Table t = Table.deserializeTable(table);
		ArrayList<Tuple> result = new ArrayList<>();
		if(t.getIndices().containsKey(t.TableName+"-"+column)){
			BTree tree = t.getIndices().get(t.TableName+"-"+column);
			switch (operator){
				case "=" :
					LinkedList<Pointer> pointers = tree.getEqualKeys((Comparable) value);
					ArrayList<Integer> x = new ArrayList<>();
					for(Pointer pointer: pointers){
						int ziad = (int) pointer.value();
						x.add(ziad);
					}
					ArrayList<Integer> pageNumbers = new ArrayList<>();
					for(int i=0; i< x.size(); i++){
						if(i==0){
							pageNumbers.add(x.get(i));
						}
						boolean ismail = false;
						for(int j=0; j< pageNumbers.size(); j++){
							if(x.get(i) == pageNumbers.get(j)){
								ismail = true;
							}
						}
						if(!ismail){
							pageNumbers.add(i);
						}
					}
					for(int page : pageNumbers){
						String pagename= t.PageNames.get(page);
						Page p = Page.deserializePage(pagename);
						Vector<Tuple> tuples = p.getTuples();
						p.serializePage();
						for(Tuple tuple : tuples){
							if(tuple.getValue(column).equals(value)){
								result.add(tuple);
							}
						}
					}break;
				case "!=" :
					LinkedList<Pointer> pointers1 = tree.getNotEqualKeys((Comparable) value);
					ArrayList<Integer> x1 = new ArrayList<>();
					for(Pointer pointer: pointers1){
						int ziad = (int) pointer.value();
						x1.add(ziad);
					}
					ArrayList<Integer> pageNumbers1 = new ArrayList<>();
					for(int i=0; i< x1.size(); i++){
						if(i==0){
							pageNumbers1.add(x1.get(i));
						}
						boolean ismail = false;
						for(int j=0; j< pageNumbers1.size(); j++){
							if(x1.get(i) == pageNumbers1.get(j)){
								ismail = true;
							}
						}
						if(!ismail){
							pageNumbers1.add(i);
						}
					}
					for(int page : pageNumbers1){
						String pagename= t.PageNames.get(page);
						Page p = Page.deserializePage(pagename);
						Vector<Tuple> tuples = p.getTuples();
						p.serializePage();
						for(Tuple tuple : tuples){
							if(!(tuple.getValue(column).equals(value))){
								result.add(tuple);
							}
						}
					}break;
				case "<" :
					LinkedList<Pointer> pointers2 = tree.getLessThanKeys((Comparable) value);
					ArrayList<Integer> x2 = new ArrayList<>();
					for(Pointer pointer: pointers2){
						int ziad = (int) pointer.value();
						x2.add(ziad);
					}
					ArrayList<Integer> pageNumbers2 = new ArrayList<>();
					for(int i=0; i< x2.size(); i++){
						if(i==0){
							pageNumbers2.add(x2.get(i));
						}
						boolean ismail = false;
						for(int j=0; j< pageNumbers2.size(); j++){
							if(x2.get(i) == pageNumbers2.get(j)){
								ismail = true;
							}
						}
						if(!ismail){
							pageNumbers2.add(i);
						}
					}
					for(int page : pageNumbers2){
						String pagename= t.PageNames.get(page);
						Page p = Page.deserializePage(pagename);
						Vector<Tuple> tuples = p.getTuples();
						p.serializePage();
						for(Tuple tuple : tuples){
							if(value instanceof String){
								String columnvalue = (String) tuple.getValue(column);
								String actualvalue = (String) value;
								if(columnvalue.compareTo(actualvalue) <0){
									result.add(tuple);
								}
							}else{
								if(value instanceof Double){
									Double columnvalue = (Double) tuple.getValue(column);
									Double actualvalue = (Double) value;
									if(columnvalue < actualvalue){
										result.add(tuple);
									}
								}else{
									int columnvalue = (int) tuple.getValue(column);
									int actualvalue = (int) value;
									if(columnvalue < actualvalue){
										result.add(tuple);
									}
								}
							}
						}
					}break;
				case ">" :
					LinkedList<Pointer> pointers3 = tree.getMoreThanKeys((Comparable) value);
					ArrayList<Integer> x3 = new ArrayList<>();
					for(Pointer pointer: pointers3){
						int ziad = (int) pointer.value();
						x3.add(ziad);
					}
					ArrayList<Integer> pageNumbers3 = new ArrayList<>();
					for(int i=0; i< x3.size(); i++){
						if(i==0){
							pageNumbers3.add(x3.get(i));
						}
						boolean ismail = false;
						for(int j=0; j< pageNumbers3.size(); j++){
							if(x3.get(i) == pageNumbers3.get(j)){
								ismail = true;
							}
						}
						if(!ismail){
							pageNumbers3.add(i);
						}
					}
					for(int page : pageNumbers3){
						String pagename= t.PageNames.get(page);
						Page p = Page.deserializePage(pagename);
						Vector<Tuple> tuples = p.getTuples();
						p.serializePage();
						for(Tuple tuple : tuples){
							if(value instanceof String){
								String columnvalue = (String) tuple.getValue(column);
								String actualvalue = (String) value;
								if(columnvalue.compareTo(actualvalue) >0){
									result.add(tuple);
								}
							}else{
								if(value instanceof Double){
									Double columnvalue = (Double) tuple.getValue(column);
									Double actualvalue = (Double) value;
									if(columnvalue > actualvalue){
										result.add(tuple);
									}
								}else{
									int columnvalue = (int) tuple.getValue(column);
									int actualvalue = (int) value;
									if(columnvalue > actualvalue){
										result.add(tuple);
									}
								}
							}
						}
					}break;
				case ">=" :
					LinkedList<Pointer> pointers4 = tree.getMoreThanOrEqualKeys((Comparable) value);
					ArrayList<Integer> x4 = new ArrayList<>();
					for(Pointer pointer: pointers4){
						int ziad = (int) pointer.value();
						x4.add(ziad);
					}
					ArrayList<Integer> pageNumbers4 = new ArrayList<>();
					for(int i=0; i< x4.size(); i++){
						if(i==0){
							pageNumbers4.add(x4.get(i));
						}
						boolean ismail = false;
						for(int j=0; j< pageNumbers4.size(); j++){
							if(x4.get(i) == pageNumbers4.get(j)){
								ismail = true;
							}
						}
						if(!ismail){
							pageNumbers4.add(i);
						}
					}
					for(int page : pageNumbers4){
						String pagename= t.PageNames.get(page);
						Page p = Page.deserializePage(pagename);
						Vector<Tuple> tuples = p.getTuples();
						p.serializePage();
						for(Tuple tuple : tuples){
							if(value instanceof String){
								String columnvalue = (String) tuple.getValue(column);
								String actualvalue = (String) value;
								if(columnvalue.compareTo(actualvalue) >=0){
									result.add(tuple);
								}
							}else{
								if(value instanceof Double){
									Double columnvalue = (Double) tuple.getValue(column);
									Double actualvalue = (Double) value;
									if(columnvalue >= actualvalue){
										result.add(tuple);
									}
								}else{
									int columnvalue = (int) tuple.getValue(column);
									int actualvalue = (int) value;
									if(columnvalue >= actualvalue){
										result.add(tuple);
									}
								}
							}
						}
					}break;
				default :
					LinkedList<Pointer> pointers5 = tree.getLessThanOrEqualKeys(((Comparable) value));
					ArrayList<Integer> x5 = new ArrayList<>();
					for(Pointer pointer: pointers5){
						int ziad = (int) pointer.value();
						x5.add(ziad);
					}
					ArrayList<Integer> pageNumbers5 = new ArrayList<>();
					for(int i=0; i< x5.size(); i++){
						if(i==0){
							pageNumbers5.add(x5.get(i));
						}
						boolean ismail = false;
						for(int j=0; j< pageNumbers5.size(); j++){
							if(x5.get(i) == pageNumbers5.get(j)){
								ismail = true;
							}
						}
						if(!ismail){
							pageNumbers5.add(i);
						}
					}
					for(int page : pageNumbers5){
						String pagename= t.PageNames.get(page);
						Page p = Page.deserializePage(pagename);
						Vector<Tuple> tuples = p.getTuples();
						p.serializePage();
						for(Tuple tuple : tuples){
							if(value instanceof String){
								String columnvalue = (String) tuple.getValue(column);
								String actualvalue = (String) value;
								if(columnvalue.compareTo(actualvalue) <=0){
									result.add(tuple);
								}
							}else{
								if(value instanceof Double){
									Double columnvalue = (Double) tuple.getValue(column);
									Double actualvalue = (Double) value;
									if(columnvalue <= actualvalue){
										result.add(tuple);
									}
								}else{
									int columnvalue = (int) tuple.getValue(column);
									int actualvalue = (int) value;
									if(columnvalue <= actualvalue){
										result.add(tuple);
									}
								}
							}
						}
					}break;


			}
		}else{
		for(int i=0; i< t.PageNames.size(); i++){
			Page p = Page.deserializePage(t.PageNames.get(i));
			Vector<Tuple> tuples = p.getTuples();
			Tuple TCheck = tuples.get(0);
			if(i==0){
				Map<String, Object> values = new HashMap<>();
				values = TCheck.getValues();
				boolean checkColumn = false;
				for(String key : values.keySet()){
					if (Objects.equals(key, column)) {
						checkColumn = true;
						break;
					}
				}
				if(!checkColumn){
					throw new DBAppException("non existing column");
				}
				if(!(((TCheck.getValue(column) instanceof Integer) && (value instanceof Integer)) || ((TCheck.getValue(column) instanceof String) && (value instanceof String)) || ((TCheck.getValue(column) instanceof Double) && (value instanceof Double)))){
					throw new DBAppException("wrong value type not matching column type");
				}
			}
			p.serializePage();
			for(Tuple tuple : tuples){
				switch (operator) {
					case "=":
						if(tuple.getValue(column).equals(value)){
							result.add(tuple);
						}
						break;
					case "!=" :
						if(!(tuple.getValue(column).equals(value))){
							result.add(tuple);
						}
						break;
					case "<" :
						if(value instanceof String){
							String compareValue = (String) tuple.getValue(column);
							String actualValue = (String) value;
							if(compareValue.compareTo(actualValue) < 0){
								result.add(tuple);
							}
						}else {
							if (value instanceof Double) {
								Double compareValue = (Double) tuple.getValue(column);
								Double actualValue = (Double) value;
								if (compareValue < actualValue) {
									result.add(tuple);
								}
							}else{
								int compareValue = (int) tuple.getValue(column);
								int actualValue = (int) value;
								if (compareValue < actualValue) {
									result.add(tuple);
								}
							}
						}
						break;
					case ">" :
						if(value instanceof String){
							String compareValue = (String) tuple.getValue(column);
							String actualValue = (String) value;
							if(compareValue.compareTo(actualValue) > 0){
								result.add(tuple);
							}
						}else {
							if (value instanceof Double) {
								Double compareValue = (Double) tuple.getValue(column);
								Double actualValue = (Double) value;
								if (compareValue > actualValue) {
									result.add(tuple);
								}
							}else{
								int compareValue = (int) tuple.getValue(column);
								int actualValue = (int) value;
								if (compareValue > actualValue) {
									result.add(tuple);
								}
							}
						}
						break;
					case "<=" :
						if(value instanceof String){
							String compareValue = (String) tuple.getValue(column);
							String actualValue = (String) value;
							if(compareValue.compareTo(actualValue) <= 0){
								result.add(tuple);
							}
						}else {
							if (value instanceof Double) {
								Double compareValue = (Double) tuple.getValue(column);
								Double actualValue = (Double) value;
								if (compareValue <= actualValue) {
									result.add(tuple);
								}
							}else{
								int compareValue = (int) tuple.getValue(column);
								int actualValue = (int) value;
								if (compareValue <= actualValue) {
									result.add(tuple);
								}
							}
						}
						break;
					default :
						if(value instanceof String){
							String compareValue = (String) tuple.getValue(column);
							String actualValue = (String) value;
							if(compareValue.compareTo(actualValue) >= 0){
								result.add(tuple);
							}
						}else {
							if (value instanceof Double) {
								Double compareValue = (Double) tuple.getValue(column);
								Double actualValue = (Double) value;
								if (compareValue >= actualValue) {
									result.add(tuple);
								}
							}else{
								int compareValue = (int) tuple.getValue(column);
								int actualValue = (int) value;
								if (compareValue >= actualValue) {
									result.add(tuple);
								}
							}
						}
				}

			}

		}


	}
		return result;
	}


	public ArrayList<Tuple> getFinalResultSet(ArrayList<Tuple> l1, ArrayList<Tuple> l2, String arrOperator){
		ArrayList<Tuple> result = new ArrayList<>();
		switch (arrOperator) {
			case "AND" :
				for(int i=0; i< l1.size(); i++){
					Tuple t1 = l1.get(i);
					boolean exist =false;
					for(int j=0; j<l2.size(); j++){
						Tuple t2 = l2.get(j);
						if(t1.equals(t2)){
							exist=true;
						}
					}
					if(exist){
						result.add(t1);
					}

				}
				break;
			case "OR" :
				for(int i=0; i< l1.size(); i++){
					Tuple t1 = l1.get(i);
					result.add(t1);
				}
				for(int i=0; i<l2.size(); i++){
					Tuple t2=l2.get(i);
					boolean check=false;
					for(int j=0; j<l1.size();j++){
						Tuple t3 = l1.get(j);
						if(t2.equals(t3)){
							check = true;
						}
					}
					if(!check){
						result.add(t2);
					}
				}
				break;
			default :
				for(int i=0; i<l1.size(); i++){
					Tuple t1 = l1.get(i);
					boolean check = false;
					for(int j=0; j<l2.size();j++){
						Tuple t2 = l2.get(i);
						if(t1.equals(t2)){
							check=true;
						}
					}
					if(!check){
						result.add(t1);
					}
				}
				for(int i=0; i<l2.size(); i++){
					Tuple t2= l2.get(i);
					boolean check = false;
					for(int j=0; j<l1.size(); j++){
						Tuple t1 = l1.get(j);
						if(t2.equals(t1)){
							check = true;
						}
					}
					if(!check){
						result.add(t2);
					}
				}
		}
		return result;
	}









	public static void main( String[] args ){
	
	try{
			String strTableName = "Student";
			DBApp	dbApp = new DBApp( );
			
			Hashtable htblColNameType = new Hashtable( );
			htblColNameType.put("id", "java.lang.Integer");
			htblColNameType.put("name", "java.lang.String");
			htblColNameType.put("gpa", "java.lang.Double");
			dbApp.createTable( strTableName, "id", htblColNameType );
			dbApp.createIndex( strTableName, "gpa", "gpaIndex" );

			Hashtable htblColNameValue = new Hashtable( );
			htblColNameValue.put("id", new Integer( 2343432 ));
			htblColNameValue.put("name", new String("Ahmed Noor" ) );
			htblColNameValue.put("gpa", new Double( 0.95 ) );
			dbApp.insertIntoTable( strTableName , htblColNameValue );


			htblColNameValue.clear( );
			htblColNameValue.put("id", new Integer( 453455 ));
			htblColNameValue.put("name", new String("Ahmed Noor" ) );
			htblColNameValue.put("gpa", new Double( 0.95 ) );
			dbApp.insertIntoTable( strTableName , htblColNameValue );
			//dbApp.deleteFromTable(strTableName,htblColNameValue);


			htblColNameValue.clear( );
			htblColNameValue.put("id", new Integer( 5674567 ));
			htblColNameValue.put("name", new String("Dalia Noor" ) );
		    htblColNameValue.put("gpa", new Double( 1.25 ) );
			dbApp.insertIntoTable( strTableName , htblColNameValue );

			htblColNameValue.clear( );
			htblColNameValue.put("id", new Integer( 23498 ));
			htblColNameValue.put("name", new String("John Noor" ) );
			htblColNameValue.put("gpa", new Double( 1.5 ) );
			dbApp.insertIntoTable( strTableName , htblColNameValue );



			htblColNameValue.clear( );
			htblColNameValue.put("id", new Integer( 78452 ));
			htblColNameValue.put("name", new String("Zaky Noor" ) );
			htblColNameValue.put("gpa", new Double( 0.88 ) );
			dbApp.insertIntoTable( strTableName , htblColNameValue );

		    Page p= Page.deserializePage("Student1");
		    System.out.println(p.getTuples());
		htblColNameValue.clear( );
		//htblColNameValue.put("id", new Integer( 78452 ));
		htblColNameValue.put("name", new String("Zaky Noor" ) );
		htblColNameValue.put("gpa", new Double( 0.50 ) );
		//dbApp.updateTable(strTableName,"78452",htblColNameValue);
		    System.out.println(p.getTuples());
			SQLTerm[] arrSQLTerms;
			arrSQLTerms = new SQLTerm[2];
			SQLTerm s1 = new SQLTerm(newTableName, id, ">", 5);
			SQLTerm s2 = new SQLTerm(newTableName, id, ">", 5);
			arrSQLTerms[0] = s1;
			arrSQLTerms[1] = s2;
			arrSQLTerms[0]._strTableName =  "Student";
			arrSQLTerms[0]._strColumnName=  "name";
			arrSQLTerms[0]._strOperator  =  "=";
			arrSQLTerms[0]._objValue     =  "John Noor";

			arrSQLTerms[1]._strTableName =  "Student";
			arrSQLTerms[1]._strColumnName=  "gpa";
			arrSQLTerms[1]._strOperator  =  "=";
			arrSQLTerms[1]._objValue     =  new Double( 1.5 );

			String[]strarrOperators = new String[1];
			strarrOperators[0] = "OR";
			// select * from Student where name = "John Noor" or gpa = 1.5;
			Iterator resultSet = dbApp.selectFromTable(arrSQLTerms , strarrOperators);
			//System.out.println(resultSet);
		while (resultSet.hasNext()) {
			Tuple tuple =(Tuple) resultSet.next();
			System.out.println(tuple);// Assuming Tuple class overrides toString()
			System.out.println("this is select");
		}

		Table t=Table.deserializeTable(strTableName);
		//System.out.println(t.getBTrees());
		}
		catch(Exception exp){
			exp.printStackTrace( );
		}
	}

	public ArrayList<String> getMyTables() {
		return TableNames;
	}
}