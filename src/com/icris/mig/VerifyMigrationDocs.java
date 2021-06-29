package com.icris.mig;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Iterator;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import java.util.HashMap;
import java.util.ArrayList;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Calendar;

import com.filenet.api.core.ObjectStore;
import com.filenet.api.query.SearchSQL;
import com.filenet.api.query.SearchScope;
import com.filenet.api.core.Document;
import com.filenet.api.core.ContentElement;
import com.filenet.api.core.ContentTransfer;
import com.filenet.api.core.Annotation;
import com.filenet.api.property.*;
import com.filenet.api.collection.*;
import com.filenet.api.constants.RefreshMode;
import com.filenet.api.query.*;
import com.icris.util.ICRISLogger;
import com.icris.util.CPEUtil;
import com.icris.util.ICRISException;
import com.icris.util.CSVParser;

class IndexAnnotationRecord {
	Double	docid;
	Integer	classnum;
	HashMap<String, String>			indexArray;
	HashMap<Integer, Integer> 	annotCount;
	Integer	pageCount;
}

public class VerifyMigrationDocs {
	static String docidSetId;
	static ICRISLogger log;
	static FileOutputStream verifiedSuccesOutputDataFile;
	static FileOutputStream verifiedFailedOutputDataFile;
	static SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yyyy");
	static private CPEUtil revampedCPEUtil;
	static private ObjectStore objectStore = null;
	static private CSVParser csvParser = new CSVParser();
	static private HashMap<Integer, ArrayList<String>> classIndexMap = new HashMap<Integer, ArrayList<String>>();
	static private HashMap<Integer, String> classNumToSymNameMap = new HashMap<Integer, String>();
	static FileOutputStream invalidAnnotSizeReport_A, invalidAnnotSizeReport_O;
	static Double widthThreshold = 0.0;
	static Double heigthThreshold = 0.0;
	static String unmatchedIndexInfo;
//	static private String[] classSymNameArray = {"","","","","",""};
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		initialize(args);
		try {
			String docidSetFilePath = "." + File.separator + "data" + File.separator + "docidSetOutput" + File.separator + docidSetId +".dat";
			BufferedReader br = new BufferedReader(new FileReader(new File(docidSetFilePath)));
			String line;
			log.info(String.format("Start verifying ... %s", docidSetId));
			while ((line=br.readLine())!=null) {
				IndexAnnotationRecord indexAnnotRec = parseIndexAnnotationRecord(line);
				if (indexAnnotRec==null) continue;
				verifyIndexAnnotation(indexAnnotRec);			
			}
			log.info(String.format("End verifying ... %s", docidSetId));
			verifiedSuccesOutputDataFile.close();
			verifiedFailedOutputDataFile.close();
		} catch (IOException e) {
			log.error(e.getMessage());
		}	catch (Exception e) {
			e.printStackTrace();
		}
	}
	private static void initialize(String[] args) {
		if (args.length>0) {
			docidSetId = args[0];
		} else {
			docidSetId = "docidSet001";
		}
		
		if (args.length > 1) {
			widthThreshold = Double.valueOf(args[1]);
			heigthThreshold = Double.valueOf(args[2]);
		} else {
			widthThreshold = 0.1;
			heigthThreshold = 0.1;
		}
		classNumToSymNameMap.put(1, "ICRIS_Pend_Doc");
		classNumToSymNameMap.put(2, "ICRIS_Reg_Doc");
		classNumToSymNameMap.put(3, "ICRIS_Tmplt_Doc");
		classNumToSymNameMap.put(4, "ICRIS_Migrate_Doc");
		classNumToSymNameMap.put(5, "ICRIS_CR_Doc");
		classNumToSymNameMap.put(6, "ICRIS_BR_Doc");
		try {
			log = new ICRISLogger(docidSetId,"verifiedBatches");
			revampedCPEUtil = new CPEUtil("revamped.server.conf", log);
			objectStore = revampedCPEUtil.getObjectStore();
			String verifiedSuccessOutputFilePath = "." + File.separator + "data" + File.separator + "verifiedOutput" + File.separator + docidSetId +"_success.dat";
			String verifiedFailedOutputFilePath = "." + File.separator + "data" + File.separator + "verifiedOutput" + File.separator + docidSetId +"_failed.dat";
			Files.deleteIfExists(Paths.get(verifiedSuccessOutputFilePath));
			verifiedSuccesOutputDataFile = new FileOutputStream(verifiedSuccessOutputFilePath);
			Files.deleteIfExists(Paths.get(verifiedFailedOutputFilePath));
			verifiedFailedOutputDataFile = new FileOutputStream(verifiedFailedOutputFilePath);
			loadClassIndexMap();
			String invalidAnnotSizeReportFilePath_A = "." + File.separator + "logs" + File.separator + "invalidAnnotSizeReports" + File.separator + "invalidAnnotSize_" + docidSetId +"_A.dat";
			Files.deleteIfExists(Paths.get(invalidAnnotSizeReportFilePath_A));
			invalidAnnotSizeReport_A = new FileOutputStream(invalidAnnotSizeReportFilePath_A);
			String invalidAnnotSizeReportFilePath_O = "." + File.separator + "logs" + File.separator + "invalidAnnotSizeReports" + File.separator + "invalidAnnotSize_" + docidSetId +"_O.dat";
			Files.deleteIfExists(Paths.get(invalidAnnotSizeReportFilePath_O));
			invalidAnnotSizeReport_O = new FileOutputStream(invalidAnnotSizeReportFilePath_O);
		} catch (ICRISException e) {
			if (e.exceptionCode.equals(ICRISException.ExceptionCodeValue.CPE_CONFIG_FILE_NOT_FOUND)) {
				log.error(e.getMessage());				
			} else if (e.exceptionCode.equals(ICRISException.ExceptionCodeValue.CPE_CONFIG_FILE_CANNOT_BE_OPENED)) {
				log.error(e.getMessage());				
			}
		} catch (IOException e) {
			log.error(e.getMessage());
		} catch (Exception e) {
			log.error("unhandled Exception");
			log.error(e.toString());
		}
	}	
	static private void verifyIndexAnnotation(IndexAnnotationRecord indexAnnotRec) throws Exception {
		Boolean result = true;
		String errMsg = String.format("%.0f, %d : ", indexAnnotRec.docid, indexAnnotRec.classnum);
		Document doc = searchDoc(String.format("%.0f",indexAnnotRec.docid), classNumToSymNameMap.get(indexAnnotRec.classnum));
		if (doc==null) {
			result = false;
			errMsg = String.format("%s docid not found;", errMsg);
		} else {
			
			if (!verifyIndex(doc, classNumToSymNameMap.get(indexAnnotRec.classnum) , indexAnnotRec.indexArray)){
				result = false;
				errMsg = String.format("%s index not matched %s;", errMsg, unmatchedIndexInfo);
			}
			
			if (!verifyAnnotation(doc, classNumToSymNameMap.get(indexAnnotRec.classnum), indexAnnotRec.annotCount)) {
				result = false;
				errMsg = String.format("%s annotation count not matched;", errMsg);
			}
			
			if (!verifyPagesCount(doc, classNumToSymNameMap.get(indexAnnotRec.classnum), indexAnnotRec.pageCount)) {
				result = false;
				errMsg = String.format("%s pages count not matched;", errMsg);
			}
			
			AnnotationSet annotSet = doc.get_Annotations();
			Iterator annotIt =  doc.get_Annotations().iterator();
			while (annotIt.hasNext()) {
				validateAnnotSize((Annotation) annotIt.next());
			}
		}
		if (result) {
			verifiedSuccesOutputDataFile.write(String.format("%s,%.0f,%d,success\n", doc.get_Id().toString(), indexAnnotRec.docid, indexAnnotRec.classnum).getBytes());
		} else {
			errMsg = String.format("%s\n", errMsg);
			verifiedFailedOutputDataFile.write(errMsg.getBytes());
		}
	}
	
	static private Boolean verifyIndex(Document doc, String classSymName , HashMap<String, String> indexArray) {
//		System.out.println("===================================");
		unmatchedIndexInfo="";
		Boolean result = true;
		doc.fetchProperties(indexArray.keySet().toArray(new String[0]));
		doc.fetchProperties(new String[] {"Id"});
		Properties properties = doc.getProperties();
		for (String indexName: indexArray.keySet()){
			switch (indexName) {
				case "F_DOCNUMBER":
					if (indexName.equals("F_DOCNUMBER")) {
					System.out.println(indexName + "," + indexArray.get(indexName) + "," + properties.getFloat64Value(indexName));
					if (!Double.valueOf(indexArray.get(indexName)).equals(properties.getFloat64Value(indexName))){
						result = false;
						}
					}
					break;
				case "DOC_SHT_FRM":
				case "DOC_BARCODE":
				case "PRT_VW_IND":
				case "PRV_DOCID":
//				case "FNP_ARCHIVE":
				case "STATUS":
				case "LTST_VER":
				case "TMPLT_ID":
					if (!"".equals(indexArray.get(indexName)) && !indexArray.get(indexName).equals(properties.getStringValue(indexName))) {
						result = false;
						String sTemp = properties.getStringValue(indexName);
						if (sTemp!=null)
							unmatchedIndexInfo = String.format("%s/%s/%s", indexName, indexArray.get(indexName), sTemp);
						else
							unmatchedIndexInfo = String.format("%s/%s/<NULL>", indexName, indexArray.get(indexName));
						System.out.println("....." + unmatchedIndexInfo);
					}
					break;
				case "FLING_DATE":
				case "FNP_ARCHIVE":
					if (!"".equals(indexArray.get(indexName)) && !compareDate(indexArray.get(indexName),properties.getDateTimeValue(indexName))) {
						result = false;
						Date dtTemp = properties.getDateTimeValue(indexName);
						if (dtTemp!=null)
							unmatchedIndexInfo = String.format("%s/%s/%s", indexName, indexArray.get(indexName), sdf.format(dtTemp));
						else 
							unmatchedIndexInfo = String.format("%s/%s/<NULL>", indexName, indexArray.get(indexName));
						System.out.println("....." + unmatchedIndexInfo);
					}
					break;
				default:
					break;
			}
			if (!result) break;
		}
		return result;
		
	}
	
	static private Boolean verifyAnnotation(Document doc, String classSymName , HashMap<Integer, Integer> annotCount) {
		HashMap<Integer, Integer> _annotCount = new HashMap<Integer, Integer>();
		doc.fetchProperties(new String[]{"Annotations"});
		AnnotationSet as = doc.get_Annotations();
		Iterator<Annotation> it = as.iterator();
		while (it.hasNext()) {
			Annotation annot = it.next();
			annot.fetchProperties(new String[]{"AnnotatedContentElement"});
			Integer ace = annot.get_AnnotatedContentElement();
			if (_annotCount.containsKey(ace)) {
				Integer i = _annotCount.get(ace);
				_annotCount.put(ace, i+1);
			} else {
				_annotCount.put(ace,1);
			}
		}
		return (_annotCount.equals(annotCount));
		
	}
	
	static private Boolean verifyPagesCount(Document doc, String classSymName ,Integer pageCount) {
		doc.fetchProperties(new String[]{"ContentElements"});
		ContentElementList cel = doc.get_ContentElements();
		return (pageCount==cel.size());		
	}
	
	static private Boolean compareDate(String dateStr, Date dateTime){
		Boolean result = true;
		try {
		Calendar calendar1 = Calendar.getInstance();
		Calendar calendar2 = Calendar.getInstance();
		calendar1.setTime(sdf.parse(dateStr));
		calendar2.setTime(dateTime);
		if (calendar1.get(Calendar.YEAR)==calendar2.get(Calendar.YEAR)&&
				calendar1.get(Calendar.MONTH)==calendar2.get(Calendar.MONTH) &&
						calendar1.get(Calendar.DAY_OF_MONTH)==calendar2.get(Calendar.DAY_OF_MONTH)
				){
			result = true;
		} else {
			result = false; 
		}
		} catch (Exception e){
			result = false;
		}
		return result;
	}
	
	static private IndexAnnotationRecord parseIndexAnnotationRecord(String strIndexAnnotRec) {
		try {
			IndexAnnotationRecord indexAnnotRec = new IndexAnnotationRecord();
			String[] tokenizedIndexAnnotRec = csvParser.lineParser(strIndexAnnotRec);
			indexAnnotRec.docid = Double.parseDouble(tokenizedIndexAnnotRec[0]);
			indexAnnotRec.classnum = Integer.parseInt(tokenizedIndexAnnotRec[1]);
			String strAnnotCountRec = tokenizedIndexAnnotRec[2];
			indexAnnotRec.annotCount = parseAnnotCount(strAnnotCountRec);
			indexAnnotRec.pageCount = Integer.parseInt(tokenizedIndexAnnotRec[3]);
			indexAnnotRec.indexArray = new HashMap<String, String>();
			ArrayList<String> indexes = classIndexMap.get(indexAnnotRec.classnum);
			for(Integer i=0; i<indexes.size(); ++i){
				indexAnnotRec.indexArray.put(indexes.get(i), tokenizedIndexAnnotRec[i+4]);
			}
			return indexAnnotRec;
		} catch (NumberFormatException e) {
			log.error(e.getMessage() + " : " + strIndexAnnotRec);
			return null;
		}
	}
	
	static private Document searchDoc(String docNum, String docClass) {
		SearchSQL searchSQL = new SearchSQL();
		SearchScope searchScope = new SearchScope(revampedCPEUtil.getObjectStore());
		searchSQL.setFromClauseInitialValue(docClass, null, true);
		searchSQL.setSelectList("F_DOCNUMBER");
		searchSQL.setMaxRecords(1);
		searchSQL.setOrderByClause("F_DOCNUMBER" + " ASC");
		String whereClause = "F_DOCNUMBER"+"=" + docNum;
		searchSQL.setWhereClause(whereClause);
		DocumentSet ds = (DocumentSet)searchScope.fetchObjects(searchSQL, null, null, false);
		Iterator it = ds.iterator();
		if (it.hasNext()){
			return (Document)it.next();
		} else {
			log.error(docNum + " not exists");
			return null;
		}
	}
	
	
	static private void loadClassIndexMap() throws IOException {
		for (Integer i=0; i<6; ++i) {
			Integer classNum = i + 1;			
			String indexNameFilePath = "." + File.separator + "config" + File.separator + "index_name_list_" + String.format("%d", classNum) + ".conf";
			BufferedReader br = new BufferedReader(new FileReader(new File(indexNameFilePath)));
			ArrayList<String> indexArray = new ArrayList<String>();
			String line;
			while ((line=br.readLine())!=null) {
				indexArray.add(line);
			}
			classIndexMap.put(classNum, indexArray);
		}
	}
	
	
	static private HashMap<Integer, Integer> parseAnnotCount(String strAnnotCountRec){
		HashMap<Integer, Integer> annotCounts = new HashMap<Integer, Integer>();
		for(String s:strAnnotCountRec.replaceAll("^\\[|\\]$", "").split(";")) {
			String[] strAnnotCount = s.split(":");
			if (strAnnotCount.length>1)
				annotCounts.put(Integer.parseInt(strAnnotCount[0]), Integer.parseInt(strAnnotCount[1]));
		}
		return annotCounts;
	}
	
	static private void validateAnnotSize(Annotation annot) throws Exception{
		annot.fetchProperties(new String[] {"Id", "AnnotatedObject","ContentElements"});
//		Integer annotUpdateSeq = annot.getUpdateSequenceNumber();
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
//			log.info(String.format("%010.0f, %f, %f", annotatedDoc.getProperties().getFloat64Value("F_DOCNUMBER"), width,heigth ));
			if (width < widthThreshold && heigth < heigthThreshold) {
			invalidAnnotSizeReport_A.write(String.format("%010.0f, %s, %s,%s,%f,%f\n",annotatedDoc.getProperties().getFloat64Value("F_DOCNUMBER"), annot.get_Id().toString(), annotClassName, annotPageNum,  width, heigth).getBytes());
			}

			if (width < widthThreshold || heigth < heigthThreshold) {
			invalidAnnotSizeReport_O.write(String.format("%010.0f, %s, %s,%s,%f,%f\n",annotatedDoc.getProperties().getFloat64Value("F_DOCNUMBER"), annot.get_Id().toString(), annotClassName, annotPageNum,  width, heigth).getBytes());        				
    		}
		}		
	}

}


