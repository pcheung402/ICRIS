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



public class ExportICRISTemplates {
	static private CPEUtil icris2CPEUtil;
	static private ICRISLogger log;
	static private ObjectStore objectStore = null;
    static private String startSubmissionDate;
    static private String maxNumOfeDocRetrieve;
    static private String endSubmissionDate;
    static private String outputBaseDir;
    static private Writer docsNotFoundLog = null, docsRecords=null, docsInvalid=null;
	static private SimpleDateFormat simpleDateFormat = new SimpleDateFormat("ddMMyyyyHHmmss");
	
	static ArrayList<HashMap<String,String>> PropertiesList;
	public static void main(String[] args) {
		// TODO Auto-generated method stub
	    try {
	    	initialize(args);   
	        SearchSQL sqlObject = new SearchSQL();
	        String select = "r.F_DOCNUMBER, r.TMPLT_ID, r.TMPLT_VER, r.Id";
	        sqlObject.setSelectList(select);
	        String docsClassName = "ICRIS_Tmplt_Doc";
	        String classAlias = "r";
	        Boolean subClassToo = false;
	        sqlObject.setFromClauseInitialValue(docsClassName, classAlias, subClassToo);
//	        sqlObject.setWhereClause(getSQLWhereClause());
	        String orderClause = "r.TMPLT_ID ASC";
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
	        		exportDocs((RepositoryRow)obj);
	        	}
	        }
    		docsNotFoundLog.flush();
    		docsRecords.flush();
    		docsInvalid.flush();
    	    System.out.println("Retrieve finished " + " : " + new Date());
	    } catch (Exception e){
	    	e.printStackTrace();
	    }
	}
	
	private static void initialize(String[] args) throws Exception {
		log = new ICRISLogger("CreateDocumentClass",null);
		icris2CPEUtil = new CPEUtil("revamped.server.conf", log);
		objectStore = icris2CPEUtil.getObjectStore();
//	    startSubmissionDate = args[0];
////	    maxNumOfeDocRetrieve = args[1];
//	    endSubmissionDate = args[1];
	    outputBaseDir = args[0];
	    if(!Files.exists(Paths.get(outputBaseDir + File.separator + "logs"))){
	    	Files.createDirectory(Paths.get(outputBaseDir + File.separator + "logs"));
	    }
//	    if(!Files.exists(Paths.get(outputBaseDir + File.separator + "eDocs"))){
//	    	Files.createDirectory(Paths.get(outputBaseDir + File.separator + "docs"));
//	    }
	    
		docsNotFoundLog = new BufferedWriter(new FileWriter(outputBaseDir + File.separator + "logs" + File.separator + "docsNotFound.log"));
		docsRecords = new BufferedWriter(new FileWriter(outputBaseDir + File.separator + "logs" + File.separator + "docsRecords.log"));
		docsInvalid = new BufferedWriter(new FileWriter(outputBaseDir + File.separator + "logs" + File.separator + "docsInvalid.log"));  		
	}
	
//	private static String getSQLWhereClause() {
//		String sqlWhereClause = null;
//		
//		if (startSubmissionDate.equals("*") && !endSubmissionDate.contentEquals("*")) {
//			sqlWhereClause = "r.SubmissionDateTime<=" + endSubmissionDate +"T000000Z";
//		} else if (!startSubmissionDate.equals("*") && endSubmissionDate.contentEquals("*")) {
//			sqlWhereClause = "r.SubmissionDateTime>=" + startSubmissionDate +"T000000Z";
//		} else if (!startSubmissionDate.equals("*") && !endSubmissionDate.contentEquals("*")) {
//			sqlWhereClause = "r.SubmissionDateTime<=" + endSubmissionDate +"T000000Z" + " AND " + "r.SubmissionDateTime>=" + startSubmissionDate +"T000000Z";
//		}
//		
//		return sqlWhereClause;
//	}
	
	private static void exportDocs(RepositoryRow row)  throws Exception{
//    	String caseNumber = row.getProperties().getStringValue("CaseNumber");
//    	Date submissionDateTime = row.getProperties().getDateTimeValue("SubmissionDateTime");
//    	String sSubmissionDate = simpleDateFormat.format(submissionDateTime);
		String templateId = row.getProperties().getStringValue("TMPLT_ID");
		String templateVer = row.getProperties().getStringValue("TMPLT_VER");
    	Document doc = Factory.Document.fetchInstance(objectStore, row.getProperties().getIdValue("Id"), null);
    	ContentElementList cel = doc.get_ContentElements();
    	Iterator celIt = cel.iterator();
    	if (!celIt.hasNext()) return;
    	ContentTransfer ct = (ContentTransfer)celIt.next();
    	InputStream is = null;
    	try {
    		is = ct.accessContentStream();
    		saveDoc(is, row);
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
    		docsNotFoundLog.append(templateId + "," + templateVer  + "," + errMsg + "\n");
    		docsNotFoundLog.flush();
    		return;
    	} catch (Exception e){
    		e.printStackTrace();
    		return;
    	}
    	
    	
	}
	
	private static void saveDoc(InputStream is, RepositoryRow row) throws Exception{
//    	String caseNumber = row.getProperties().getStringValue("CaseNumber");
//    	Date submissionDateTime = row.getProperties().getDateTimeValue("SubmissionDateTime");
//    	String sSubmissionDate = simpleDateFormat.format(submissionDateTime);
		String templateId = row.getProperties().getStringValue("TMPLT_ID");
		String templateVer = row.getProperties().getStringValue("TMPLT_VER");
//    	ZipInputStream zIs = new ZipInputStream(is);
//    	ZipEntry zEntry = zIs.getNextEntry();
//    	String sZipEntry = zEntry.getName();
//    	String formType = sZipEntry.substring(0, sZipEntry.lastIndexOf(".vxf")).substring(4);
//    	String fileName = caseNumber + "$" + formType + "$" + sSubmissionDate + ".vxf";
//    	String fileName = row.getProperties().getFloat64Value("F_DOCNUMBER").toString() +".doc";
    	String fileName = String.format("%.0f", row.getProperties().getFloat64Value("F_DOCNUMBER"))+".dot";
//    	System.out.println("fileName  : " + fileName);
    	String docFolderFullPath = getDocFolderFullPath(templateId);
//    	System.out.println("path  : " + eDocFolderFullPath);
//    	while (Files.exists(Paths.get(docFolderFullPath + File.separator + fileName))){
//    		fileName = fileName + "$";
//    	}
    	try{
    	Files.copy(is , Paths.get(docFolderFullPath + File.separator + fileName), StandardCopyOption.REPLACE_EXISTING);
    	docsRecords.append(String.format("%.0f", row.getProperties().getFloat64Value("F_DOCNUMBER")) + "," +templateId + "," + templateVer + "\n");
    	docsRecords.flush();
    	} catch (EOFException e) {
    		docsInvalid.append(String.format("%.0f", row.getProperties().getFloat64Value("F_DOCNUMBER"))+","+templateId + "," + templateVer  + "," + e.getMessage() + "\n");
    		docsInvalid.flush();
    		return;
    	} catch (ZipException e){
    		docsInvalid.append(String.format("%.0f", row.getProperties().getFloat64Value("F_DOCNUMBER"))+","+templateId + "," + templateVer   + "," + e.getMessage()+ "\n");
    		docsInvalid.flush();
    		return;
    	}
    	catch (Exception e){
    		System.out.println(docFolderFullPath + File.separator + fileName);
    		e.printStackTrace();
    		return;
    	}		
	}
	
	private static String getDocFolderFullPath(String templateId) throws Exception{
		String docRootFolder = outputBaseDir;
		return docRootFolder;
	}
}
