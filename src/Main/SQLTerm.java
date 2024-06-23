package Main;

/** * @author Wael Abouelsaadat */

public class SQLTerm {

	public String _strTableName,_strColumnName, _strOperator;
	public Object _objValue;

	public SQLTerm(String newTableName, String id, String s, Object o){
		_strTableName=newTableName;
		_strColumnName=id;
		_strOperator=s;
		_objValue=o;
	}

	public String get_strTableName(){
		return this._strTableName;
	}
	public String get_strColumnName(){
		return this._strColumnName;
	}
	public String get_strOperator(){
		return this._strOperator;
	}
	public Object get_objValue(){
		return this._objValue;
	}

}