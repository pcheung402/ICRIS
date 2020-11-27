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
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.xml.parsers.DocumentBuilder;

import com.icris.util.ICRISLogger;
import com.icris.util.CPEUtil;
import com.icris.util.CSVParser;
import com.filenet.api.constants.RefreshMode;
import com.filenet.api.core.Connection;
import com.filenet.api.core.Domain;
import com.filenet.api.core.Factory;
import com.filenet.api.core.ObjectStore;
import com.filenet.api.core.Document;
import com.filenet.api.core.Annotation;
import com.filenet.api.core.ContentTransfer;
import com.filenet.api.util.UserContext;
import com.filenet.api.admin.PropertyTemplateString;
import com.filenet.api.admin.PropertyTemplateDateTime;
import com.filenet.api.admin.PropertyTemplateFloat64;
import com.filenet.api.admin.PropertyTemplateInteger32;
import com.filenet.api.admin.LocalizedString;
import com.filenet.api.constants.Cardinality;
import com.filenet.api.query.*;
import com.filenet.api.collection.*;
import com.filenet.api.exception.*;
import com.filenet.api.property.PropertyFilter;



public class ScanAnnotationSize {
	static boolean isAND = true;
	static private CPEUtil revampedCPEUtil;
	static private ICRISLogger log;
	static private ObjectStore objectStore = null;
	static private Double widthThreshold = 0.0;
	static private Double heigthThreshold = 0.0;
	static FileOutputStream invalidAnnotSizeReport;
	static ArrayList<HashMap<String,String>> PropertiesList;
	static private SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMdd");
	static private Date curDate = new Date();
	public static void main(String[] args) {
		// TODO Auto-generated method stub
	    try {
	    	initialize(args);   
	        SearchSQL sqlObject = new SearchSQL();
	        String select = "r.Name, r.Id";
	        sqlObject.setSelectList(select);
	        String annoClassName = "Annotation";
	        String classAlias = "r";
	        Boolean subClassToo = true;
	        sqlObject.setFromClauseInitialValue(annoClassName, classAlias, subClassToo);
//	        sqlObject.setWhereClause(getSQLWhereClause());
//	        String orderClause = "r.SubmissionDateTime ASC";
//	        sqlObject.setOrderByClause(orderClause);
//	        System.out.println("SQL : " + sqlObject.toString());
	        SearchScope searchScope = new SearchScope(objectStore);
//	        System.out.println ("Start retrieving : " + new Date());
	        RepositoryRowSet rowSet = searchScope.fetchRows(sqlObject, null, null, new Boolean(true));        
	        PageIterator pageIter= rowSet.pageIterator();
	        pageIter.setPageSize(1000);
	        while (pageIter.nextPage()) {
        		System.out.println("Scanning next " + pageIter.getElementCount() + " annotations  :" + new Date());
	        	for (Object obj : pageIter.getCurrentPage()) {
	        		RepositoryRow row = (RepositoryRow)obj;
	        		Annotation annot = Factory.Annotation.fetchInstance(objectStore, row.getProperties().getIdValue("Id"), null);
	        		annot.fetchProperties(new String[] {"Id"});
//	        		Integer annotUpdateSeq = annot.getUpdateSequenceNumber();
	        		Document annotatedDoc = (Document)annot.get_AnnotatedObject();
	        		annotatedDoc.fetchProperties(new String[] {"Id","F_DOCNUMBER"});
	        		ContentElementList cel = annot.get_ContentElements();
	        		Iterator celIt = cel.iterator();
	        		while(celIt.hasNext()) {
	        			ContentTransfer ct = (ContentTransfer)celIt.next();
	        			DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
	        			DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
	        			org.w3c.dom.Document domDoc = dBuilder.parse(ct.accessContentStream());
	        			NodeList classNodeList = domDoc.getElementsByTagName("PropDesc");
	        			Node nNode = classNodeList.item(0);
	        			NamedNodeMap nodeMap = nNode.getAttributes();
	        			Double width = Double.valueOf(nodeMap.getNamedItem("F_WIDTH").getNodeValue());
	        			Double heigth = Double.valueOf(nodeMap.getNamedItem("F_HEIGHT").getNodeValue());
	        			String annotClassName = nodeMap.getNamedItem("F_CLASSNAME").getNodeValue();
	        			String annotPageNum = nodeMap.getNamedItem("F_PAGENUMBER").getNodeValue();
	        			if (isAND) {
		        			if (width < widthThreshold && heigth < heigthThreshold) {
	//	        			System.out.println(String.format("%010.0f, %s, %s, %d, %f, %f",annotatedDoc.getProperties().getFloat64Value("F_DOCNUMBER"),annotClassName, annotPageNum, annotUpdateSeq, width, heigth));
	//	        				System.out.println(String.format("%010.0f, %s, %s, %f, %f",annotatedDoc.getProperties().getFloat64Value("F_DOCNUMBER"),annotClassName, annotPageNum,  width, heigth));
		        			invalidAnnotSizeReport.write(String.format("%010.0f, %s, %s,%s,%f,%f\n",annotatedDoc.getProperties().getFloat64Value("F_DOCNUMBER"), annot.get_Id().toString(), annotClassName, annotPageNum,  width, heigth).getBytes());
		        			}
	        			} else {

		        			if (width < widthThreshold || heigth < heigthThreshold) {
	//	        			System.out.println(String.format("%010.0f, %s, %s, %d, %f, %f",annotatedDoc.getProperties().getFloat64Value("F_DOCNUMBER"),annotClassName, annotPageNum, annotUpdateSeq, width, heigth));
	//	        				System.out.println(String.format("%010.0f, %s, %s, %f, %f",annotatedDoc.getProperties().getFloat64Value("F_DOCNUMBER"),annotClassName, annotPageNum,  width, heigth));
		        			invalidAnnotSizeReport.write(String.format("%010.0f, %s, %s,%s,%f,%f\n",annotatedDoc.getProperties().getFloat64Value("F_DOCNUMBER"), annot.get_Id().toString(), annotClassName, annotPageNum,  width, heigth).getBytes());
		        			}	        				
		        		}
	        		}
	        	}
	        }
//    		eDocsNotFoundLog.flush();
//    		eDocsRecords.flush();
//    		eDocsInvalid.flush();
	        invalidAnnotSizeReport.close();
    	    System.out.println("Scanning finished " + " : " + new Date());
	    } catch (Exception e){
	    	e.printStackTrace();
	    }
	}
	
	private static void initialize(String[] args) throws Exception {
		log = new ICRISLogger("annotScan" + simpleDateFormat.format(curDate),"invalidAnnotSizeReports");
		revampedCPEUtil = new CPEUtil("revamped.server.conf", log);
		objectStore = revampedCPEUtil.getObjectStore();
//		annotSizeThreshold = Double.valueOf(args[0]);
		widthThreshold = Double.valueOf(args[0]);
		heigthThreshold = Double.valueOf(args[1]);
		String invalidAnnotSizeReportFilePath = "." + File.separator + "logs" + File.separator + "invalidAnnotSizeReports" + File.separator + "invalidAnnotSize_" +simpleDateFormat.format(curDate) +"_A.dat";;
		if (args.length > 2) {
			if (args[2].equals("-A")) {
				isAND = true;
			} else {
				isAND = false ;
				invalidAnnotSizeReportFilePath = "." + File.separator + "logs" + File.separator + "invalidAnnotSizeReports" + File.separator + "invalidAnnotSize_" +simpleDateFormat.format(curDate) +"_O.dat";
			}
		}
		Files.deleteIfExists(Paths.get(invalidAnnotSizeReportFilePath));
		invalidAnnotSizeReport = new FileOutputStream(invalidAnnotSizeReportFilePath);
	}
	

}
