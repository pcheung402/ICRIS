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
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.file.*;
import java.util.*;
import java.io.FileNotFoundException;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.text.ParseException;
import java.util.concurrent.*;
import javax.security.auth.Subject;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Result;
import javax.xml.transform.Source;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

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
import com.filenet.api.replication.ReplicationGroup;
import com.icris.util.*;


public class AnnotConverter implements Runnable {
	private String batchSetId;
	private CPEUtil revampedCPEUtil;
	private CSVParser csvParser;
	private ICRISLogger log;
	private Date batchStartTime;
	private Date batchEndTime;
	private Boolean previewOnly;
//	private StoragePolicy destStoragePolicy;
//	private StorageArea destStorageArea;
	private String datFileName;
	private FileOutputStream annotConvertOutputDataFile;
	private DocumentBuilderFactory factory;
	private DocumentBuilder builder;
	private Big5ToUTFMapper big5ToUTF16Mapper;

//	private HashMap<Integer, String> classNumToSymNameMap = new HashMap<Integer, String>();
	
	
	public void run( ) {
		if(Objects.isNull(this.revampedCPEUtil)) System.out.println("revampedCPEUtil is null");
		UserContext.get().pushSubject(this.revampedCPEUtil.getSubject());

		this.waitToStart();
		try {
			this.startBatchSet();
		} catch (ICRISException e) {
			log.error("No more standby Storage Area available");
		}
	}
	
	public AnnotConverter(String batchSetId, String datFileName, ICRISLogger log, FileOutputStream ofs, CPEUtil revampedCPEUtil) {
		// TODO Auto-generated constructor stub
		
		this.log = log;
		this.batchSetId = batchSetId;
		this.datFileName = datFileName;
		this.csvParser = new CSVParser();
		this.annotConvertOutputDataFile = ofs;

		this.revampedCPEUtil = revampedCPEUtil;
		try {
//			factory = DocumentBuilderFactory.newInstance();
//			this.builder = factory.newDocumentBuilder();
			java.util.Properties props = new java.util.Properties();
//			props.load(new FileInputStream("config/batchSetConfig/" + this.batchSetId + ".conf"));
			
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
			log.error(e.getStackTrace().toString());
		}
	}
	
	public void startBatchSet() throws ICRISException {
		log.info(String.format("starting,%s/%s",batchSetId,datFileName));
		Boolean isOverdue=false;
		Integer pages = 0;
		Double lastDocNumberMoved=0.0, firstDocNumberMoved=0.0;
		BufferedReader reader;
 		
		try {
			reader = new BufferedReader(new FileReader("data/annotConvertBatches/batchSets/" + this.batchSetId +"/" + this.datFileName));
			for (String line; (line = reader.readLine()) != null; ){
				Date date = new Date();
				if (date.after(batchEndTime)) {
					isOverdue = true;
					break;
				}
				String[] parsedLine = csvParser.lineParser(line);
				processParsedLine(parsedLine);
			}
			reader.close();

			if (isOverdue) {
				log.info(String.format("overdue,%s/%s",batchSetId,datFileName));
			} else {
				log.info(String.format("finished,%s/%s",batchSetId,datFileName));
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
	

	private void loadBatchSetConfig() throws ICRISException {
		SimpleDateFormat formatter = new SimpleDateFormat("dd-MM-yyyy HH:mm:ss");
		try {
			this.revampedCPEUtil = new CPEUtil("revamped.server.conf", log);
			java.util.Properties props = new java.util.Properties();
			props.load(new FileInputStream("config/batchSetConfig/" + this.batchSetId + ".conf"));
			this.batchStartTime = formatter.parse(props.getProperty("batchStartTime"));
			this.batchEndTime = formatter.parse(props.getProperty("batchEndTime"));
			this.previewOnly = "TRUE".equalsIgnoreCase(props.getProperty("PreviewOnly", "False"))?Boolean.TRUE:Boolean.FALSE;
		} catch (IOException e) {
			throw new ICRISException(ICRISException.ExceptionCodeValue.BM_LOAD_BATCH_SET_CONFIG_ERROR,"Load batchSetConfig error : " + "config/batchSetConfig/" + this.batchSetId + ".conf", e);
		} catch (ParseException e) {
			throw new ICRISException(ICRISException.ExceptionCodeValue.BM_LOAD_BATCH_SET_CONFIG_ERROR,"BatchSetConfig time format error ", e);			
		} catch (NumberFormatException e) {
			throw new ICRISException(ICRISException.ExceptionCodeValue.BM_LOAD_BATCH_SET_CONFIG_ERROR,"BatchSetConfig integer format error", e);						
		} catch (ParserConfigurationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private void processParsedLine(String[] parsedLine) {
		String docNum = parsedLine[1];
		String docClass ="ICRIS_Barcode_Doc";

		
		SearchSQL searchSQL = new SearchSQL();
		SearchScope searchScope = new SearchScope(revampedCPEUtil.getObjectStore());
		searchSQL.setFromClauseInitialValue(docClass, null, true);
		searchSQL.setSelectList("DOC_BARCODE, F_DOCNUMBER, Name, Annotations, ClassDescription, ReplicationGroup, Id");
		searchSQL.setMaxRecords(99);
		String whereClause = "F_DOCNUMBER"+"=" + docNum;
		searchSQL.setWhereClause(whereClause);
		DocumentSet docSet = (DocumentSet) searchScope.fetchObjects(searchSQL, null, null, null);
		Iterator<Document>  itDoc = docSet.iterator();
		while (itDoc.hasNext()) {
			Document doc = itDoc.next();
			if(!doc.get_ClassDescription().get_Name().endsWith("_WORM")) {
				
				doc.fetchProperties(new String[] {"Id", "DOC_BARCODE","F_DOCNUMBER"});
//				if(!doc.get_Annotations().iterator().hasNext()) System.out.printf("%s, %s, %s does not have annotations\n",docNum, doc.get_ClassDescription().get_Name(), doc.get_Id().toString());
				this.revampedCPEUtil.updateAnnot(doc, this.annotConvertOutputDataFile, this.previewOnly);
			}
		} 

	}
	
}



