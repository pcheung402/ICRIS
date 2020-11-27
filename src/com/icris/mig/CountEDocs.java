package com.icris.mig;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Properties;
import java.io.InputStream;
import java.util.Date;
import com.icris.util.CPEUtil;
import com.filenet.api.core.ObjectStore;
import com.filenet.api.query.*;
import com.filenet.api.collection.*;
import com.icris.util.ICRISLogger;


public class CountEDocs {
	static private CPEUtil icris2CPEUtil;
	static private ICRISLogger log;
	static private InputStream serverConfigInputStream=null;
	static private ObjectStore objectStore = null;
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
	    try {
	    	log = new ICRISLogger("",null);
			icris2CPEUtil = new CPEUtil("server.properties.icris2P8", log);
			objectStore = icris2CPEUtil.getObjectStore();       
	        SearchSQL sqlObject = new SearchSQL();
	        String select = "r.Id";
	        sqlObject.setSelectList(select);
	        String eDocsClassName = "eDocs";
	        String classAlias = "r";
	        Boolean subClassToo = false;
	        sqlObject.setFromClauseInitialValue(eDocsClassName, classAlias, subClassToo);
	        System.out.println("SQL : " + sqlObject.toString());
	        SearchScope searchScope = new SearchScope(objectStore);
	        System.out.println ("Start retrieving : " + new Date());
	        RepositoryRowSet rowSet = searchScope.fetchRows(sqlObject, null, null, new Boolean(true));
	        Double count = new Double(0);
	        Iterator itRow = rowSet.iterator();
	        while (itRow.hasNext()){
	        	if (count%1000== 0 && count>0) {
	        		System.out.print(".");
	        	}
	        	if (count%100000== 0 && count>0) {
	        		System.out.println();
	        	}
	        	RepositoryRow row = (RepositoryRow)itRow.next();
	        	count++;
	        }
    	    System.out.println("\nRetrieve finished " + " : " + count);
	    } catch (Exception e){
	    	e.printStackTrace();
	    }
	}
	

}