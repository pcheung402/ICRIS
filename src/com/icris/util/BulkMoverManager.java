package com.icris.util;

//import com.icris.mig.CPEContentBulkMover;
import com.icris.util.*;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.FileReader;
import java.io.BufferedReader;
import java.nio.file.*;
import java.util.*;
import java.io.FileNotFoundException;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.text.ParseException;
import java.util.concurrent.*;
import javax.security.auth.Subject;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.filenet.api.core.*;
import com.filenet.api.admin.*;
import com.filenet.api.constants.RefreshMode;
import com.filenet.api.constants.ResourceStatus;
import com.filenet.api.constants.SweepMode;
import com.filenet.api.sweep.CmBulkMoveContentJob;
import com.filenet.api.exception.*;
import com.filenet.api.util.*;
import com.filenet.apiimpl.constants.Constants;
import com.filenet.api.collection.*;
import com.filenet.api.exception.ExceptionCode;
import com.filenet.api.query.SearchSQL;
import com.filenet.api.query.SearchScope;
import com.icris.util.*;

public class BulkMoverManager implements Runnable {
	private String batchSetId;
	private CPEUtil revampedCPEUtil;
	private CSVParser csvParser;
	private ICRISLogger log;
	private Date batchStartTime;
	private Date batchEndTime;
//	private StoragePolicy destStoragePolicy;
//	private StorageArea destStorageArea;
	private String datFileName;
	private FileOutputStream bulkMoveOutputDataFile;
	private Boolean isByDocId = false;
	private HashMap<Integer, String> classNumToSymNameMap = new HashMap<Integer, String>();
	
	
	public void run( ) {
		UserContext.get().pushSubject(revampedCPEUtil.getSubject());
		this.waitToStart();
		try {
			this.startBatchSet();
		} catch (ICRISException e) {
			log.error("No more standby Storage Area available");
		}
	}
	
	public BulkMoverManager(String batchSetId, String datFileName, ICRISLogger log, FileOutputStream ofs,CPEUtil revampedCPEUtil, boolean isByDocid) {
		// TODO Auto-generated constructor stub
		this.classNumToSymNameMap.put(1, "ICRIS_Pend_Doc");
		this.classNumToSymNameMap.put(2, "ICRIS_Reg_Doc");
		this.classNumToSymNameMap.put(3, "ICRIS_Tmplt_Doc");
		this.classNumToSymNameMap.put(4, "ICRIS_Migrate_Doc");
		this.classNumToSymNameMap.put(5, "ICRIS_CR_Doc");
		this.classNumToSymNameMap.put(6, "ICRIS_BR_Doc");
		
		this.log = log;
		this.batchSetId = batchSetId;
		this.datFileName = datFileName;
		this.csvParser = new CSVParser();
		this.bulkMoveOutputDataFile = ofs;
		this.isByDocId = isByDocid;
		try {
			java.util.Properties props = new java.util.Properties();
			props.load(new FileInputStream("config/batchSetConfig/" + this.batchSetId + ".conf"));
			this.revampedCPEUtil = revampedCPEUtil;
			loadBatchSetConfig();
		} catch (ICRISException e) {
			if (e.exceptionCode.equals(ICRISException.ExceptionCodeValue.CPE_USNAME_PASSWORD_INVALID)) {
				log.error(e.getMessage());				
			} else if (e.exceptionCode.equals(ICRISException.ExceptionCodeValue.CPE_URI_INVALID)) {
				log.error(e.getMessage());				
			} else if (e.exceptionCode.equals(ICRISException.ExceptionCodeValue.CPE_INVALID_OS_NAME)) {
				log.error(e.getMessage());				
			} else if (e.exceptionCode.equals(com.icris.util.ICRISException.ExceptionCodeValue.BM_LOAD_BATCH_SET_CONFIG_ERROR)) {
				log.error(e.getMessage());
			}
		} catch (Exception e) {
			log.error("unhandled CPEUtil Exception");
			log.error(e.toString());
		}
	}
	
	public void startBatchSet() throws ICRISException {
		log.info(String.format("starting,%s/%s",batchSetId,datFileName));
		Boolean isOverdue=false;
		Integer numOfDocMoved = 0;
		Double lastDocNumberMoved=0.0, firstDocNumberMoved=0.0;
		BufferedReader reader;
 		
		try {
			reader = new BufferedReader(new FileReader("data/bulkMoveBatches/batchSets/" + this.batchSetId +"/" + this.datFileName));
			for (String line; (line = reader.readLine()) != null; ){
				Date date = new Date();
				if (date.after(batchEndTime)) {
					isOverdue = true;
					break;
				}
				String[] parsedLine = csvParser.lineParser(line);
				Document doc;
				if (this.isByDocId) {
					doc = searchDoc (parsedLine[0], this.classNumToSymNameMap.get(Integer.parseInt(parsedLine[1])));
				} else {
					doc = Factory.Document.fetchInstance(revampedCPEUtil.getObjectStore(), new Id(parsedLine[1]), null);
				}
				doc.refresh(new String[] {"F_DOCNUMBER","F_DOCCLASSNUMBER","Id"});
				lastDocNumberMoved = doc.getProperties().getFloat64Value("F_DOCNUMBER");
				if (numOfDocMoved==0) {
					firstDocNumberMoved = doc.getProperties().getFloat64Value("F_DOCNUMBER");
				}
				numOfDocMoved++;
				Integer retries = 0;
				for (retries= 0 ; retries < 5; ++retries) {
					try {
//						doc.moveContent(destStorageArea);
//						doc.save(RefreshMode.REFRESH);
						while (!revampedCPEUtil.moveContent(doc)) {
							try {TimeUnit.SECONDS.sleep(2);} catch (Exception _e) {_e.printStackTrace();};
						};
						bulkMoveOutputDataFile.write(String.format("%010.0f,%s/%s\n", lastDocNumberMoved, batchSetId,datFileName).getBytes());
						bulkMoveOutputDataFile.flush();
						break;
					} catch (ICRISException e) {
						throw e;
					} catch (EngineRuntimeException e) {
//						log.error("docNum=" + lastDocNumberMoved + ","  + e.getMessage());
						if (e.getExceptionCode().equals(ExceptionCode.CONTENT_FCP_OPERATION_FAILED)
								||e.getExceptionCode().equals(ExceptionCode.CONTENT_FCP_OPERATION_FAILED_ON_OPEN)
								||e.getExceptionCode().equals(ExceptionCode.CONTENT_FCP_OPERATION_FAILED_WITH_CONTENT)) {
							log.warn(String.format("CFS-IS retry #%d: %s/%s,%12.0f,%s",retries, batchSetId,datFileName,lastDocNumberMoved,e.getMessage()));
							continue;
						} else {
							log.error(String.format("unhandled error ,%s/%s,%.0f, %s",batchSetId,datFileName,lastDocNumberMoved,e.getMessage()));
							e.printStackTrace();
							break;
						}	
					}
				}
				if(retries >=5 ) {
					log.error(String.format("CFS-IS error ,%s/%s",batchSetId,datFileName,lastDocNumberMoved));
				}
			}
			reader.close();

			if (isOverdue) {
				log.info(String.format("overdue,%s/%s,%010.0f,%010.0f,%d",batchSetId,datFileName,firstDocNumberMoved,lastDocNumberMoved,numOfDocMoved));
			} else {
				log.info(String.format("finished,%s/%s,%010.0f,%010.0f,%d",batchSetId,datFileName,firstDocNumberMoved,lastDocNumberMoved,numOfDocMoved));
			}
		} catch (IOException e) {
			log.error(String.format("io exception,%s/%s",batchSetId,datFileName));
			e.printStackTrace();
		} catch (Exception e) {
			log.error(String.format("unhandled exception,%s/%s",batchSetId,datFileName));
//			log.error(datFileName + ":unhandled exception");
			e.printStackTrace();
		}
	}
	
	public void waitToStart() {
		log.info(String.format("waiting,%s/%s,%s",batchSetId,datFileName,batchStartTime.toString()));
		try {
			Date curDate = new Date();
			TimeUnit.MILLISECONDS.sleep(this.batchStartTime.getTime() - curDate.getTime());
		} catch (InterruptedException e) {
		
	}
}
	
	
//	public void examineBulkMoveSweepJobResult() {
//
//	}

	private void loadBatchSetConfig() throws ICRISException {
		SimpleDateFormat formatter = new SimpleDateFormat("dd-MM-yyyy HH:mm:ss");
		try {
			java.util.Properties props = new java.util.Properties();
			props.load(new FileInputStream("config/batchSetConfig/" + this.batchSetId + ".conf"));
			this.batchStartTime = formatter.parse(props.getProperty("batchStartTime"));
			this.batchEndTime = formatter.parse(props.getProperty("batchEndTime"));
//			this.destStoragePolicy = this.revampedCPEUtil.getStoragePolicy(props.getProperty("destStoragePolicy"));
//			StorageAreaSet sas = this.destStoragePolicy.get_StorageAreas(); 
//			Iterator it = sas.iterator();
//			/*
//			 *  find the first "Open" Storage Area
//			 */
//			while (it.hasNext()) {
//				StorageArea sa = (StorageArea)it.next();
//				if (sa.get_ResourceStatus().equals(ResourceStatus.OPEN)) {
//					this.destStorageArea = sa;
//					break;
//				}
//			}
		} catch (IOException e) {
			throw new ICRISException(ICRISException.ExceptionCodeValue.BM_LOAD_BATCH_SET_CONFIG_ERROR,"Load batchSetConfig error : " + "config/batchSetConfig/" + this.batchSetId + ".conf", e);
		} catch (ParseException e) {
			throw new ICRISException(ICRISException.ExceptionCodeValue.BM_LOAD_BATCH_SET_CONFIG_ERROR,"BatchSetConfig time format error ", e);			
		} catch (NumberFormatException e) {
			throw new ICRISException(ICRISException.ExceptionCodeValue.BM_LOAD_BATCH_SET_CONFIG_ERROR,"BatchSetConfig integer format error", e);						
		}
	}
	
	private Document searchDoc(String docNum, String docClass) {
		SearchSQL searchSQL = new SearchSQL();
		SearchScope searchScope = new SearchScope(revampedCPEUtil.getObjectStore());
		searchSQL.setFromClauseInitialValue(docClass, null, true);
		searchSQL.setSelectList("F_DOCNUMBER");
		searchSQL.setMaxRecords(1);
		searchSQL.setOrderByClause("F_DOCNUMBER" + " ASC");
		String whereClause = "F_DOCNUMBER"+"=" + docNum;
		searchSQL.setWhereClause(whereClause);
//		System.out.println("*****  " + searchSQL.toString());
		DocumentSet ds = (DocumentSet)searchScope.fetchObjects(searchSQL, null, null, false);
		Iterator it = ds.iterator();
		if (it.hasNext()){
			return (Document)it.next();
		} else {
			log.error(docNum + " not exists");
			return null;
		}
	}
}
