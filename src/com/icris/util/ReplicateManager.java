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
import com.filenet.apiimpl.constants.Constants;
import com.filenet.api.collection.*;

import com.icris.util.*;

public class ReplicateManager implements Runnable {
	private String batchSetId;
	private CPEUtil revampedCPEUtil;
	private CSVParser csvParser;
	private ICRISLogger log;
	private Date batchStartTime;
	private Date batchEndTime;
	private Boolean isByDocId;
	private String docidSymbolicName;
	private String datFileName;
	private HashMap<Integer, ArrayList<String>> classIndexMap = new HashMap<Integer, ArrayList<String>>();
	private FileOutputStream replicateOutputDataFile;
	private HashMap<Integer, String> classNumToSymNameMap = new HashMap<Integer, String>();
	private String[] isSystemProperties = {"F_ARCHIVEDATE","F_DELETEDATE","F_DOCCLASSNUMBER","F_DOCFORMAT","F_DOCLOCATION","F_DOCNUMBER","F_DOCTYPE","F_ENTRYDATE","F_PAGES","F_RETENTOFFSET"};
	
	public void run( ) {
		UserContext.get().pushSubject(revampedCPEUtil.getSubject());
		this.waitToStart();
		this.startBatchSet();
	}
	
	public ReplicateManager(String batchSetId, String datFileName, ICRISLogger log, FileOutputStream ofs,  CPEUtil revampedCPEUtil, Boolean isByDocId) throws ICRISException, IOException {
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
		this.replicateOutputDataFile = ofs;
		this.isByDocId = isByDocId;
		
		if (this.isByDocId) {
			System.out.println("By IS document number");
		} else {
			System.out.println("By P8 GUID");
		}
		

		java.util.Properties props = new java.util.Properties();
		props.load(new FileInputStream("config/replicateConfig/" + this.batchSetId + ".conf"));
		this.docidSymbolicName = props.getProperty("docidSymbolicName");
		this.revampedCPEUtil = revampedCPEUtil;
		loadBatchSetConfig();
		loadClassIndexMap();

	}
	
	public void startBatchSet() {
//		log.info("Starting Batch Set : " + batchSetId+":"+ datFileName);
		log.info(String.format("starting,%s/%s",batchSetId,datFileName));
		Boolean isOverdue=false;
		Integer numOfDocReplicated = 0;
		Double lastDocNumberRelicated=0.0, firstDocNumberReplicated=0.0;
		BufferedReader reader;
//		Integer retries = 0; 
		Date ts1, ts2,ts3;
		try {
			reader = new BufferedReader(new FileReader("data/replicateBatches/batchSets/" + this.batchSetId +"/" + this.datFileName));
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
				lastDocNumberRelicated = doc.getProperties().getFloat64Value("F_DOCNUMBER");
				if (numOfDocReplicated==0) {
					firstDocNumberReplicated = doc.getProperties().getFloat64Value("F_DOCNUMBER");
				}

				if (replicateDoc(doc)) {
					numOfDocReplicated++;
				}


			}
			reader.close();
			if (isOverdue) {
				log.info(String.format("overdue,%s/%s,%010.0f,%010.0f,%d",batchSetId,datFileName,firstDocNumberReplicated,lastDocNumberRelicated,numOfDocReplicated));
			} else {
				log.info(String.format("finished,%s/%s,%010.0f,%010.0f,%d",batchSetId,datFileName,firstDocNumberReplicated,lastDocNumberRelicated,numOfDocReplicated));
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
	
	
//	public void examineBulkMoveSweepJobResult() {
//
//	}

	private void loadBatchSetConfig() throws ICRISException {
		SimpleDateFormat formatter = new SimpleDateFormat("dd-MM-yyyy HH:mm:ss");
		try {
		java.util.Properties props = new java.util.Properties();
		props.load(new FileInputStream("config/replicateConfig/" + this.batchSetId + ".conf"));
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
	
	private Boolean replicateDoc(Document doc) {
		Document  copyDoc = null;
		String batchID;
		doc.fetchProperties(new String[]{"Id","BatchID", "StorageAreaFlag", "MimeType","ContentElements","RetrievalName","ContentType"});
		doc.fetchProperties(isSystemProperties);
		String fileName = doc.getProperties().getStringValue("DocumentTitle");
		batchID = doc.getProperties().getStringValue("BatchID");

		/*
		 * 
		 * if (BatchID == 0) => There is no WORM copy yet => create a now document object in WORM and copy the content elements from SAN copy to WORM copy
		 * 
		 * if (BatchID !=0) => There is already a WORM copy => use Batch ID to retrieve the WORM copy
		 * 
		 */
		if (batchID==null){
			try { 
			copyDoc= Factory.Document.createInstance(revampedCPEUtil.getObjectStore(), doc.getClassName() + "_WORM");
			copyDoc.set_MimeType(doc.get_MimeType());
			copyDoc.getProperties().putValue("DocumentTitle", fileName);
			copyDoc.getProperties().putValue("StorageAreaFlag", "0");
			copyDoc.save(RefreshMode.REFRESH);
			}  catch (EngineRuntimeException e) {
				if (e.getExceptionCode().equals(ExceptionCode.E_BAD_CLASSID)){
					log.error(String.format("Class %s_WORM not found  : %s, %10.0f, %s/%s",doc.getClassName(),doc.getProperties().getFloat64Value("F_DOCNUMBER"), this.batchSetId, this.datFileName));
				} else {
					log.error(String.format("%s : %s, %10.0f, %s/%s", e.getMessage(), doc.getClassName(),doc.getProperties().getFloat64Value("F_DOCNUMBER"), this.batchSetId, this.datFileName));
				}
				return false;
			}
			try {
				ContentElementList docContentList = doc.get_ContentElements();
				ContentElementList contentList = Factory.ContentElement.createList();
				Iterator it = docContentList.iterator();
				while(it.hasNext()) {
					ContentTransfer oldctObject = (ContentTransfer)it.next();
					ContentTransfer  ctObject = Factory.ContentTransfer.createInstance();
					ctObject.setCaptureSource(oldctObject.accessContentStream());
					ctObject.set_RetrievalName(oldctObject.get_RetrievalName());
					ctObject.set_ContentType(oldctObject.get_ContentType());
					contentList.add(ctObject);
				}
				copyDoc.set_ContentElements(contentList);
				copyDoc.checkin(AutoClassify.DO_NOT_AUTO_CLASSIFY, CheckinType.MAJOR_VERSION);
				doc.getProperties().putValue("BatchID", copyDoc.get_Id().toString());
				doc.getProperties().putValue("StorageAreaFlag", "0");				
			} catch (EngineRuntimeException e) {
				log.error(String.format("%s : %s, %10.0f, %s/%s", e.getMessage(), doc.getClassName(),doc.getProperties().getFloat64Value("F_DOCNUMBER"), this.batchSetId, this.datFileName));
				copyDoc.delete();
				copyDoc.save(RefreshMode.NO_REFRESH);
				return false;
			}
		} else {
			copyDoc = Factory.Document.fetchInstance(revampedCPEUtil.getObjectStore(), new Id(batchID),null);
			copyDoc.fetchProperties(new String[]{"Id","BatchID", "StorageAreaFlag", "MimeType","ContentElements","RetrievalName","RetrievalName","ContentType","F_DOCCLASSNUMBER","F_DOCNUMBER"});
			copyDoc.fetchProperties(isSystemProperties);

			/*
			 * 
			 * Clean up the annotations in WORM copy
			 * 
			 */
			try {
				AnnotationSet annos = copyDoc.get_Annotations();
				Iterator iter = annos.iterator();
				while (iter.hasNext()) {
					Annotation anno = (Annotation)iter.next();
					anno.delete();
					anno.save(RefreshMode.NO_REFRESH);
				}
			} catch (EngineRuntimeException e) {
				log.error(String.format("%s : %s, %10.0f, %s/%s", e.getMessage(), doc.getClassName(),doc.getProperties().getFloat64Value("F_DOCNUMBER"), this.batchSetId, this.datFileName));
				return false;
			} 
		}
		
		/*
		 * 
		 * copy document properties from SAN copy to WORM copy
		 * 
		 * 
		 */
		try {
			ArrayList<String> indexes = classIndexMap.get(doc.getProperties().getInteger32Value("F_DOCCLASSNUMBER"));
			doc.fetchProperties(indexes.toArray(new String[0]));
			for(String indexSymbolicName : indexes){
				copyDoc.getProperties().putObjectValue(indexSymbolicName, doc.getProperties().getObjectValue(indexSymbolicName));
			}
			
			for (String s : isSystemProperties) {
				copyDoc.getProperties().putObjectValue(s,doc.getProperties().getObjectValue(s));
			}
			
			copyDoc.getProperties().putValue("BatchID", doc.get_Id().toString());
			copyDoc.save(RefreshMode.REFRESH);	
			doc.save(RefreshMode.REFRESH);
			
			/*
			 * Copy annotations from SAN copy to WOM copy
			 */
			AnnotationSet origianlAnnos = doc.get_Annotations();
			Iterator iter = origianlAnnos.iterator();
			while (iter.hasNext()) {
				Annotation originalAnno = (Annotation)iter.next();
				originalAnno.fetchProperties(new String[] {"BatchID","Id"});
				ContentElementList cels = originalAnno.get_ContentElements();
				ContentElementList newCels = Factory.ContentElement.createList();
				Annotation annObject = Factory.Annotation.createInstance(revampedCPEUtil.getObjectStore(), "Annotation");
				Iterator<ContentElement> ceIter = cels.iterator();
				while (ceIter.hasNext()) {
					ContentElement ce = ceIter.next();
					if(ce instanceof ContentTransfer){
						InputStream is = ((ContentTransfer)ce).accessContentStream();
						ContentTransfer ctNew = Factory.ContentTransfer.createInstance();
						ctNew.setCaptureSource(updateAnnotGUID(is, copyDoc.get_Id().toString()));
						newCels.add(ctNew);
					}
				}
				annObject.set_ContentElements(newCels);
				annObject.set_Permissions(originalAnno.get_Permissions());
				annObject.set_Creator(originalAnno.get_Creator());
				annObject.set_DateCreated(originalAnno.get_DateCreated());
				annObject.getProperties().putValue("BatchID", originalAnno.get_Id().toString());
				annObject.set_AnnotatedContentElement(originalAnno.get_AnnotatedContentElement());
				annObject.set_AnnotatedObject(copyDoc);
				annObject.save(RefreshMode.REFRESH);
	
				originalAnno.getProperties().putValue("BatchID", annObject.get_Id().toString());
				originalAnno.save(RefreshMode.REFRESH);
				
			}
		}  catch (EngineRuntimeException e) {
			log.error(String.format("%s : %s, %10.0f, %s/%s", e.getMessage(), doc.getClassName(),doc.getProperties().getFloat64Value("F_DOCNUMBER"), this.batchSetId, this.datFileName));
			return false;
		}
		
		try {
			replicateOutputDataFile.write(String.format("%s,%s,%010.0f,%s/%s\n", doc.get_Id().toString(), copyDoc.get_Id().toString(),doc.getProperties().getFloat64Value("F_DOCNUMBER"), batchSetId,datFileName).getBytes());
			replicateOutputDataFile.flush();
		} catch (IOException e) {
			log.error(String.format("%s,%s/%s",e.getMessage(), batchSetId,datFileName));
		}
		return true;
	}
	
	private InputStream updateAnnotGUID(InputStream is, String targetId) {
		try {
			DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
			DocumentBuilder builder = factory.newDocumentBuilder();
			org.w3c.dom.Document doc = builder.parse(is);
			org.w3c.dom.Element element = doc.getDocumentElement();
			org.w3c.dom.NodeList nodes = doc.getElementsByTagName("PropDesc");		
			for(int k=0;k< nodes.getLength();k++){
				org.w3c.dom.Node node = nodes.item(k);
				if(node.getNodeType() == Node.ELEMENT_NODE){
					org.w3c.dom.Element e = (org.w3c.dom.Element)node;
					e.setAttribute("F_ID", targetId);
					e.setAttribute("F_ANNOTATEDID", targetId);
				}
			}
			ByteArrayOutputStream oStream = new ByteArrayOutputStream();
			Source xmlSource = new DOMSource(doc);
			Result oTarget = new StreamResult(oStream);
			TransformerFactory.newInstance().newTransformer().transform(xmlSource, oTarget);
			InputStream iS = new ByteArrayInputStream(oStream.toByteArray());
			return iS;
		} catch (Exception e) {
			return null;
		}
	}
	
	private void loadClassIndexMap() throws IOException {
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
