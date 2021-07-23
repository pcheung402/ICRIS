package com.icris.util;

//import com.icris.mig.CPEContentBulkMover;
import com.icris.util.*;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.FileReader;
import java.io.BufferedReader;
import java.nio.file.*;
import java.util.*;
import java.io.FileNotFoundException;
import java.text.SimpleDateFormat;
import java.text.ParseException;
import java.util.concurrent.*;
import javax.security.auth.Subject;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Result;
import javax.xml.transform.Source;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.TransformerFactoryConfigurationError;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.dom4j.Node;
import org.xml.sax.SAXException;

import com.filenet.api.core.*;
import com.filenet.api.admin.*;
import com.filenet.api.constants.*;
import com.filenet.api.sweep.CmBulkMoveContentJob;
import com.filenet.api.exception.*;
import com.filenet.api.query.SearchSQL;
import com.filenet.api.query.SearchScope;
import com.filenet.api.util.*;
import com.filenet.api.property.*;
import com.filenet.apiimpl.constants.Constants;
import com.filenet.api.collection.*;
import com.filenet.api.constants.RetentionConstants;

import com.icris.util.*;

public class RemoveFromWormManager implements Runnable {
	private String batchSetId;
	private CPEUtil revampedCPEUtil;
	private CSVParser csvParser;
	private ICRISLogger log;
	private Date batchStartTime;
	private Date batchEndTime;
	private Boolean isByDocId;
	private String datFileName;
	private FileOutputStream removeFromWormOutputDataFile;
	private String[] isSystemProperties = {"F_ARCHIVEDATE","F_DELETEDATE","F_DOCCLASSNUMBER","F_DOCFORMAT","F_DOCLOCATION","F_DOCNUMBER","F_DOCTYPE","F_ENTRYDATE","F_PAGES","F_RETENTOFFSET"};
	
	public void run( ) {
		UserContext.get().pushSubject(revampedCPEUtil.getSubject());
		this.waitToStart();
		this.startBatchSet();
	}
	
	public RemoveFromWormManager(String batchSetId, String datFileName, ICRISLogger log, FileOutputStream ofs,  CPEUtil revampedCPEUtil, Boolean isByDocId) throws ICRISException, IOException {
		// TODO Auto-generated constructor stub
		this.log = log;
		this.batchSetId = batchSetId;
		this.datFileName = datFileName;
		this.csvParser = new CSVParser();
		this.removeFromWormOutputDataFile = ofs;
		this.isByDocId = isByDocId;
		
		if (this.isByDocId) {
			System.out.println("By IS document number");
		} else {
			System.out.println("By P8 GUID");
		}
		

		java.util.Properties props = new java.util.Properties();
		props.load(new FileInputStream("config/removeFromWormConfig/" + this.batchSetId + ".conf"));
		this.revampedCPEUtil = revampedCPEUtil;
		loadBatchSetConfig();
//		loadClassIndexMap();

	}
	
	public void startBatchSet() {
//		log.info("Starting Batch Set : " + batchSetId+":"+ datFileName);
		log.info(String.format("starting,%s/%s",batchSetId,datFileName));
		Boolean isOverdue=false;
		Integer numOfDocRemoved = 0;
		Double lastDocNumberRemoved=0.0, firstDocNumberRemoved=0.0;
		BufferedReader reader;
//		Integer retries = 0; 
		Date ts1, ts2,ts3;
		try {
			reader = new BufferedReader(new FileReader("data/removeWormBatches/batchSets/" + this.batchSetId +"/" + this.datFileName));
			for (String line; (line = reader.readLine()) != null; ){
				Date date = new Date();
				if (date.after(batchEndTime)) {
					isOverdue = true;
					break;
				}
				String[] parsedLine = csvParser.lineParser(line);
//				System.out.println("****  "+parsedLine[0] );
				Document doc;
				if (this.isByDocId) {
					doc = searchDoc (parsedLine[0],"ICRIS_DOC");
				} else {
					doc = Factory.Document.fetchInstance(revampedCPEUtil.getObjectStore(), new Id(parsedLine[1]), null);
				}
				
				doc.refresh(new String[] {"F_DOCNUMBER","F_DOCCLASSNUMBER","DocumentTitle","BatchID","StorageAreaFlag"});
				lastDocNumberRemoved = doc.getProperties().getFloat64Value("F_DOCNUMBER");
				if (numOfDocRemoved==0) {
					firstDocNumberRemoved = doc.getProperties().getFloat64Value("F_DOCNUMBER");
				}

				if (removeDoc(doc)) {
					numOfDocRemoved++;
				}


			}
			reader.close();
			if (isOverdue) {
				log.info(String.format("overdue,%s/%s,%010.0f,%010.0f,%d",batchSetId,datFileName,firstDocNumberRemoved,lastDocNumberRemoved,numOfDocRemoved));
			} else {
				log.info(String.format("finished,%s/%s,%010.0f,%010.0f,%d",batchSetId,datFileName,firstDocNumberRemoved,lastDocNumberRemoved,numOfDocRemoved));
			}			
			
		} catch (IOException e) {
			log.error(String.format("%s,%s/%s",e.getMessage(), batchSetId,datFileName));
			e.printStackTrace();
		} 
	}
	
	public void waitToStart() {
	log.info(String.format("waiting,%s/%s,%s",batchSetId,datFileName,batchStartTime.toLocaleString()));
	try {
		Date curDate = new Date();
		TimeUnit.MILLISECONDS.sleep(this.batchStartTime.getTime() - curDate.getTime());
	} catch (InterruptedException e) {	
		log.info(String.format("interrupted ,%s/%s",batchSetId,datFileName));
	}
}
	

	private void loadBatchSetConfig() throws ICRISException {
		SimpleDateFormat formatter = new SimpleDateFormat("dd-MM-yyyy HH:mm:ss");
		try {
		java.util.Properties props = new java.util.Properties();
		props.load(new FileInputStream("config/removeFromWormConfig/" + this.batchSetId + ".conf"));
		System.out.println(props.getProperty("batchStartTime")+"/"+props.getProperty("batchEndTime"));
		this.batchStartTime = formatter.parse(props.getProperty("batchStartTime"));
		this.batchEndTime = formatter.parse(props.getProperty("batchEndTime"));
//		this.batchConcurrency = Integer.valueOf(props.getProperty("batchConcurrency"));
		} catch (IOException e) {
			throw new ICRISException(ICRISException.ExceptionCodeValue.BM_LOAD_BATCH_SET_CONFIG_ERROR,"Load batchSetConfig error : " + "config/batchSetConfig/" + this.batchSetId + ".conf", e);
		} catch (ParseException e) {
			throw new ICRISException(ICRISException.ExceptionCodeValue.BM_LOAD_BATCH_SET_CONFIG_ERROR,"BatchSetConfig time format error ", e);			
		} catch (NumberFormatException e) {
			throw new ICRISException(ICRISException.ExceptionCodeValue.BM_LOAD_BATCH_SET_CONFIG_ERROR,"BatchSetConfig integer format error", e);						
		}
	}
	
	private Boolean removeDoc(Document doc) {
		Document	sanDoc = null;
		Boolean		retResult = false;
		String batchID;
		doc.fetchProperties(new String[]{"Id","BatchID", "StorageAreaFlag", "MimeType","ContentElements","RetrievalName","ContentType"});
		doc.fetchProperties(isSystemProperties);
		batchID = doc.getProperties().getStringValue("BatchID");
		
		/*
		 * 
		 * if (BatchID == 0) => This is a zombie WORM copy => no need to clean the batch ID of SAN copy
		 * 
		 * if (BatchID !=0) => There is a SAN copy corresponding to this WORM copy => clean the Batch ID of SAN copy 
		 * 
		 */
		try {
			if (batchID!=null){
				sanDoc = Factory.Document.fetchInstance(revampedCPEUtil.getObjectStore(), new Id(batchID),null);
				sanDoc.fetchProperties(new String[]{"Id","BatchID", "StorageAreaFlag", "MimeType","ContentElements","RetrievalName","ContentType"});
				sanDoc.fetchProperties(isSystemProperties);
				sanDoc.getProperties().putObjectValue("BatchID", null);
				sanDoc.getProperties().putObjectValue("StorageAreaFlag", null);
				sanDoc.save(RefreshMode.NO_REFRESH);
			} 
			
			doc.delete();
			doc.save(RefreshMode.NO_REFRESH);
			removeFromWormOutputDataFile.write(String.format("%s,%s,%010.0f,%s/%s\n", doc.get_Id().toString(), sanDoc.get_Id().toString(),doc.getProperties().getFloat64Value("F_DOCNUMBER"), batchSetId,datFileName).getBytes());
			removeFromWormOutputDataFile.flush();
			retResult = true;

		} catch (EngineRuntimeException e) {
			retResult = false;
			e.printStackTrace();
			log.error(String.format("Cannot remove from WORM: %s,%010.0f,%s/%s", doc.get_Id().toString(), doc.getProperties().getFloat64Value("F_DOCNUMBER"), batchSetId,datFileName));			
		} catch (IOException e){
			retResult = true;
			log.error(String.format("%s,%s/%s",e.getMessage(), batchSetId,datFileName));
		}
		
		return retResult;
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
