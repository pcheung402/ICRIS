package com.icris.mig;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.Properties;
import java.io.InputStream;
import javax.security.auth.Subject;
import java.nio.file.*;
import java.util.zip.*;
import java.util.Date;
import java.text.SimpleDateFormat;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.Writer;
import java.io.EOFException;
import java.util.zip.ZipException;

import com.icris.util.CPEUtil;
import com.icris.util.CSVParser;
import com.icris.util.ICRISLogger;
import com.filenet.api.constants.RefreshMode;
import com.filenet.api.core.Connection;
import com.filenet.api.core.Domain;
import com.filenet.api.core.Factory;
import com.filenet.api.core.ObjectStore;
import com.filenet.api.core.Document;
import com.filenet.api.core.ContentTransfer;
import com.filenet.api.util.UserContext;
import com.filenet.api.admin.PropertyTemplateString;
import com.filenet.api.admin.PropertyTemplateDateTime;
import com.filenet.api.admin.PropertyTemplateFloat64;
import com.filenet.api.admin.PropertyTemplateInteger32;
import com.filenet.api.collection.LocalizedStringList;
import com.filenet.api.admin.LocalizedString;
import com.filenet.api.constants.Cardinality;
import com.filenet.api.query.*;
import com.filenet.api.collection.*;
import com.filenet.api.exception.*;



public class ExportEDocs {
	static private CPEUtil icris2CPEUtil;
	static private ICRISLogger log;
	static private ObjectStore objectStore = null;
    static private String startSubmissionDate;
    static private String maxNumOfeDocRetrieve;
    static private String endSubmissionDate;
    static private String outputBaseDir;
    static private Writer eDocsNotFoundLog = null, eDocsRecords=null, eDocsInvalid=null;
	static private SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMdd_HHmmss");
	
	static ArrayList<HashMap<String,String>> PropertiesList;
	public static void main(String[] args) {
		// TODO Auto-generated method stub
	    try {
	    	initialize(args);   
	        SearchSQL sqlObject = new SearchSQL();
	        String select = "r.Name, r.DocumentTitle, r.CaseNumber, r.SubmissionDateTime, r.Id";
	        sqlObject.setSelectList(select);
	        String eDocsClassName = "eDocs";
	        String classAlias = "r";
	        Boolean subClassToo = false;
	        sqlObject.setFromClauseInitialValue(eDocsClassName, classAlias, subClassToo);
	        sqlObject.setWhereClause(getSQLWhereClause());
	        String orderClause = "r.SubmissionDateTime ASC";
	        sqlObject.setOrderByClause(orderClause);
	        System.out.println("SQL : " + sqlObject.toString());
	        SearchScope searchScope = new SearchScope(objectStore);
	        System.out.println ("Start retrieving : " + new Date());
	        RepositoryRowSet rowSet = searchScope.fetchRows(sqlObject, null, null, new Boolean(true));        
	        PageIterator pageIter= rowSet.pageIterator();
	        pageIter.setPageSize(1000);
	        while (pageIter.nextPage()) {
        		System.out.println("Retrieving next " + pageIter.getElementCount() + " records  :" + new Date());
	        	for (Object obj : pageIter.getCurrentPage()) {
	        		exportEDoc((RepositoryRow)obj);
	        	}
	        }
    		eDocsNotFoundLog.flush();
    		eDocsRecords.flush();
    		eDocsInvalid.flush();
    	    System.out.println("Retrieve finished " + " : " + new Date());
	    } catch (Exception e){
	    	e.printStackTrace();
	    }
	}
	
	private static void initialize(String[] args) throws Exception {
		log = new ICRISLogger("CreateDocumentClass",null);
		icris2CPEUtil = new CPEUtil("server.properties.icris2P8",log);
		objectStore = icris2CPEUtil.getObjectStore();
	    startSubmissionDate = args[0];
//	    maxNumOfeDocRetrieve = args[1];
	    endSubmissionDate = args[1];
	    outputBaseDir = args[2];
	    if(!Files.exists(Paths.get(outputBaseDir + File.separator + "logs"))){
	    	Files.createDirectory(Paths.get(outputBaseDir + File.separator + "logs"));
	    }
	    if(!Files.exists(Paths.get(outputBaseDir + File.separator + "eDocs"))){
	    	Files.createDirectory(Paths.get(outputBaseDir + File.separator + "eDocs"));
	    }
	    
		eDocsNotFoundLog = new BufferedWriter(new FileWriter(outputBaseDir + File.separator + "logs" + File.separator + "eDocsNotFound.log"));
		eDocsRecords = new BufferedWriter(new FileWriter(outputBaseDir + File.separator + "logs" + File.separator + "eDocsRecords.log"));
		eDocsInvalid = new BufferedWriter(new FileWriter(outputBaseDir + File.separator + "logs" + File.separator + "eDocsInvalid.log"));  		
	}
	
	private static String getSQLWhereClause() {
		String sqlWhereClause = null;
		
		if (startSubmissionDate.equals("*") && !endSubmissionDate.contentEquals("*")) {
			sqlWhereClause = "r.SubmissionDateTime<=" + endSubmissionDate +"T000000Z";
		} else if (!startSubmissionDate.equals("*") && endSubmissionDate.contentEquals("*")) {
			sqlWhereClause = "r.SubmissionDateTime>=" + startSubmissionDate +"T000000Z";
		} else if (!startSubmissionDate.equals("*") && !endSubmissionDate.contentEquals("*")) {
			sqlWhereClause = "r.SubmissionDateTime<" + endSubmissionDate +"T000000Z" + " AND " + "r.SubmissionDateTime>=" + startSubmissionDate +"T000000Z";
		}
		
		return sqlWhereClause;
	}
	
	private static void exportEDoc(RepositoryRow row)  throws Exception{
    	String caseNumber = row.getProperties().getStringValue("CaseNumber");
    	Date submissionDateTime = row.getProperties().getDateTimeValue("SubmissionDateTime");
    	String sSubmissionDate = simpleDateFormat.format(submissionDateTime);
    	Document doc = Factory.Document.fetchInstance(objectStore, row.getProperties().getIdValue("Id"), null);
    	ContentElementList cel = doc.get_ContentElements();
    	Iterator celIt = cel.iterator();
    	if (!celIt.hasNext()) return;
    	ContentTransfer ct = (ContentTransfer)celIt.next();
//    	InputStream is = null;
    	try {
//    		is = ct.accessContentStream();
    		saveEDoc(ct, row);
    	} catch (EngineRuntimeException e) {
    		String errMsg = null;
    		if(e.getExceptionCode().equals(ExceptionCode.CONTENT_DCA_CONTENT_ELEMENT_NOT_FOUND)) {
    			errMsg = "CONTENT_DCA_CONTENT_ELEMENT_NOT_FOUND";
    		} else if (e.getExceptionCode().equals(ExceptionCode.CONTENT_STREAM_INIT_FAILED)){
    			errMsg = "CONTENT_STREAM_INIT_FAILED";
    		} else if (e.getExceptionCode().equals(ExceptionCode.RETRIEVE_SQL_SYNTAX_ERROR)){
    			errMsg = "Invalid Date Format";
    		}else {
    			e.printStackTrace();
    		}
    		eDocsNotFoundLog.append(caseNumber + "," + sSubmissionDate  + "," + errMsg + "\n");
    		eDocsNotFoundLog.flush();
    		return;
    	} catch (Exception e){
    		e.printStackTrace();
    		return;
    	}
    	
    	
	}
	
	private static void saveEDoc(ContentTransfer ct, RepositoryRow row) throws Exception{
		InputStream is = ct.accessContentStream();
		Double ct_size = ct.get_ContentSize();
		Double is_size = getStreamSize(is);
		is = ct.accessContentStream();
    	String caseNumber = row.getProperties().getStringValue("CaseNumber");
    	Date submissionDateTime = row.getProperties().getDateTimeValue("SubmissionDateTime");
    	String sSubmissionDate = simpleDateFormat.format(submissionDateTime);
//    	System.out.println("********   " +ct_size.toString() +"," + is_size.toString());
    	ZipInputStream zIs = new ZipInputStream(is);
    	ZipEntry zEntry = zIs.getNextEntry();
    	String sZipEntry = zEntry.getName();
    	String formType = sZipEntry.substring(0, sZipEntry.lastIndexOf(".vxf")).substring(4);
    	String fileName = "eDocs_" + caseNumber + "_" + formType + "_" + sSubmissionDate + ".vxf";
//    	System.out.println("fileName  : " + fileName);
    	String eDocFolderFullPath = getEDocFolderFullPath(sSubmissionDate);
//    	System.out.println("path  : " + eDocFolderFullPath);
    	if (Math.abs(ct_size-is_size) > 1.0) {
    		eDocsInvalid.append(caseNumber + "," + sSubmissionDate  + "," + "Document Size Not Matched" + "," +ct_size.toString() +  "," + is_size.toString() + "\n");
    		eDocsInvalid.flush();
    		return;    		
    	}
    	while (Files.exists(Paths.get(eDocFolderFullPath + File.separator + fileName))){
    		fileName = fileName + "$";
    	}
    	try{
    	Files.copy(zIs , Paths.get(eDocFolderFullPath + File.separator + fileName), StandardCopyOption.REPLACE_EXISTING);
    	eDocsRecords.append(fileName + "," + caseNumber + "," + sSubmissionDate + "," + formType + "," + is_size.toString() + "\n");
    	eDocsRecords.flush();
    	} catch (EOFException e) {
    		eDocsInvalid.append(caseNumber + "," + sSubmissionDate  + "," + e.getMessage() + "\n");
    		eDocsInvalid.flush();
    		return;
    	} catch (ZipException e){
    		eDocsInvalid.append(caseNumber + "," + sSubmissionDate   + "," + e.getMessage()+ "\n");
    		eDocsInvalid.flush();
    		return;
    	}
    	catch (Exception e){
    		e.printStackTrace();
    		return;
    	}		
	}
	
	private static  Double getStreamSize(InputStream is) {
		byte[] bytes = new byte[1024];
		int count;
		Double size = 0.0;
		try {
			while ((count = is.read(bytes)) > 0) {
			    size += count;		    
			}
		} catch (Exception e) {
			size = 0.0;
		}

		return size;
	}
	
	private static String getEDocFolderFullPath(String sSubmissionDate) throws Exception{
		String dd = sSubmissionDate.substring(6, 8);
		String mm = sSubmissionDate.substring(4, 6);
		String yyyy = sSubmissionDate.substring(0, 4);
		String eDocRootFolder = outputBaseDir + File.separator + "eDocs";
	    if(!Files.exists(Paths.get(eDocRootFolder + File.separator + yyyy))){
	    	Files.createDirectory(Paths.get(eDocRootFolder + File.separator + yyyy));
	    }
	    
	    if(!Files.exists(Paths.get(eDocRootFolder + File.separator + yyyy + File.separator + mm))){
	    	Files.createDirectory(Paths.get(eDocRootFolder + File.separator + yyyy + File.separator + mm));
	    }
	    
		return eDocRootFolder + File.separator + yyyy + File.separator + mm;
	}
}
