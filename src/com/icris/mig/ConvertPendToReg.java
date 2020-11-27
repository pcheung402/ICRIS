package com.icris.mig;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.BufferedReader;
import java.io.IOException;
import java.util.Iterator;

import com.filenet.api.core.ObjectStore;
import com.filenet.api.query.SearchSQL;
import com.filenet.api.query.SearchScope;
import com.filenet.api.core.Document;
import com.filenet.api.collection.*;
import com.filenet.api.constants.RefreshMode;
import com.filenet.api.query.*;
import com.icris.util.ICRISLogger;
import com.icris.util.CPEUtil;
import com.icris.util.CSVParser;
import com.icris.util.ICRISException;

public class ConvertPendToReg {
	static String docidSetId;
	static ICRISLogger log;
	static private CPEUtil revampedCPEUtil;
	static private ObjectStore objectStore = null;
	static private CSVParser csvParser = new CSVParser();
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		initialize(args);
		try {
		String registeredDocidSetFilePath = "." + File.separator + "data" + File.separator + "regDocidSetOutput" + File.separator + docidSetId +".dat";
		BufferedReader br = new BufferedReader(new FileReader(new File(registeredDocidSetFilePath)));
		String line;
		while ((line=br.readLine())!=null) {
			String[] tokenizedLine = csvParser.lineParser(line);
			Document pendDoc = searchPendDoc(tokenizedLine[0]);
			if (pendDoc!=null) {
				convertToReg(pendDoc);
			}
		}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	private static void initialize(String[] args) {
		if (args.length>0) {
			docidSetId = args[0];
		} else {
			docidSetId = "docidSet001";
		}

		try {
			log = new ICRISLogger(docidSetId, "convertToRegBatches");
			revampedCPEUtil = new CPEUtil("revamped.server.conf", log);
			objectStore = revampedCPEUtil.getObjectStore();
		} catch (ICRISException e) {
			if (e.exceptionCode.equals(ICRISException.ExceptionCodeValue.CPE_CONFIG_FILE_NOT_FOUND)) {
				log.error(e.getMessage());				
			} else if (e.exceptionCode.equals(ICRISException.ExceptionCodeValue.CPE_CONFIG_FILE_CANNOT_BE_OPENED)) {
				log.error(e.getMessage());				
			}
		} catch (Exception e) {
			log.error("unhandled Exception");
			log.error(e.toString());
		}		
	}
	
	static private Document searchPendDoc(String docNum) {
		if (!isNumeric(docNum)) {
			log.error(docNum + " is invalid (not numeric)");
			return null;			
		}
		SearchSQL searchSQL = new SearchSQL();
		SearchScope searchScope = new SearchScope(revampedCPEUtil.getObjectStore());
		searchSQL.setFromClauseInitialValue("ICRIS_Pend_Doc", null, true);
		searchSQL.setSelectList("F_DOCNUMBER");
		searchSQL.setMaxRecords(1);
		searchSQL.setOrderByClause("F_DOCNUMBER" + " ASC");
		String whereClause = "F_DOCNUMBER"+"=" + docNum;
		searchSQL.setWhereClause(whereClause);
//		log.info(searchSQL.toString());
		DocumentSet ds = (DocumentSet)searchScope.fetchObjects(searchSQL, null, null, false);
		Iterator it = ds.iterator();
		if (it.hasNext()){
			return (Document)it.next();
		} else {
			log.error(docNum + " not exists");
			return null;
		}
	}
	
	static private void convertToReg(Document pendDoc) {
		pendDoc.changeClass("ICRIS_Reg_Doc");
		pendDoc.save(RefreshMode.REFRESH);
		String props[] = {"Name","F_DOCNUMBER"};
		pendDoc.fetchProperties(props);
		log.info("Change " + pendDoc.getProperties().getFloat64Value("F_DOCNUMBER").toString() + " to Registered");
	}
	
	static private Boolean isNumeric(String str) {
		return str.chars().allMatch(Character::isDigit);
	}
}
