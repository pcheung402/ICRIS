package com.icris.util;

//import com.icris.mig.CPEContentBulkMover;
import com.icris.util.*;

import oracle.net.aso.r;

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
import java.sql.*;
import java.text.SimpleDateFormat;
import java.text.ParseException;
import java.util.concurrent.*;
import java.util.function.Consumer;

import javax.security.auth.Subject;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.Result;
import javax.xml.transform.Source;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.dom4j.Node;

import com.filenet.api.core.*;
import com.filenet.api.admin.*;
import com.filenet.api.constants.AutoClassify;
import com.filenet.api.constants.CheckinType;
import com.filenet.api.constants.RefreshMode;
import com.filenet.api.constants.ResourceStatus;
import com.filenet.api.constants.RetentionConstants;
import com.filenet.api.constants.SweepMode;
import com.filenet.api.sweep.CmBulkMoveContentJob;
import com.filenet.api.exception.*;
import com.filenet.api.util.*;
import com.filenet.apiimpl.constants.Constants;
import com.filenet.api.collection.*;
import com.filenet.api.query.SearchSQL;
import com.filenet.api.query.SearchScope;
import com.filenet.api.query.RepositoryRow;
import com.icris.util.*;

public class BulkMoverManager_PI_Reg implements Runnable {
	private String batchSetId;
	private CPEUtil revampedCPEUtil;
	private CSVParser csvParser;
	private ICRISLogger log;
	private java.util.Date batchStartTime;
	private java.util. Date batchEndTime;
	private String datFileName;
	private FileOutputStream bulkMoveOutputDataFile;
	private String JDBCURL; 
	private String dbuser;
	private String dbpassword; 
	private java.sql.Connection conn;
	private String queryString;
	private Boolean previewOnly;
	
	
	public void run( ) {
		UserContext.get().pushSubject(revampedCPEUtil.getSubject());
		this.waitToStart();
		try {
			this.conn = DriverManager.getConnection(JDBCURL, dbuser, dbpassword);
			this.startBatchSet();
			this.conn.close();
		} catch (IOException e) {
			log.error(String.format("io exception,%s/%s",batchSetId,datFileName));
			e.printStackTrace();
		} catch (ICRISException e) {
			log.error("No more standby Storage Area available");
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
	}
	
	public BulkMoverManager_PI_Reg(String batchSetId, String datFileName, ICRISLogger log, FileOutputStream ofs,CPEUtil revampedCPEUtil) {
		// TODO Auto-generated constructor stub
	
		this.log = log;
		this.batchSetId = batchSetId;
		this.datFileName = datFileName;
		this.csvParser = new CSVParser();
		this.bulkMoveOutputDataFile = ofs;
		try {
			this.revampedCPEUtil = revampedCPEUtil;
			loadConfig();
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
	
	public void startBatchSet() throws ICRISException, IOException, SQLException {
		log.info(String.format("starting,%s/%s",batchSetId,datFileName));
		Boolean isOverdue=false;
//		Integer pages = 0;
		BufferedReader reader;
 		
//			this.numOfDocMoved = 0;

//
//			read batch data file, each line contains a DOC_GRP_NO
//
			reader = new BufferedReader(new FileReader("data/bulkMoveBatches_PI_Reg/batchSets/" + this.batchSetId +"/" + this.datFileName));
			for (String line; (line = reader.readLine()) != null; ){// each line contains one DOC_GRP_NO
//				
//				check batch end time
//				
				java.util.Date date = new java.util.Date();
				if (date.after(batchEndTime)) { // if batch end time is reached, end the batch
					isOverdue = true;
					break;
				}

				String[] parsedLine = csvParser.lineParser(line);
				Integer doc_grp_no = Integer.parseInt(parsedLine[0], 10);
				String mainFormDocBarcode = null;

				
//				Get the list of document image for the <doc_group_no>
//				
//				search all records in CR_DOC_RLTSHP table for records by DOC_GRP_NO, LNK_TYP=8, ordered by PRMRY_IND (descending) and CASE_NO (ascending)
//				
				PreparedStatement queryStatement = this.conn.prepareStatement(this.queryString,  ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
				queryStatement.setInt(1, doc_grp_no);
				
//			
//				the result set contains DOC_BARCODE if the same DOC_GRP_NO, the result set is sorted by 
//				
				ResultSet rs = queryStatement.executeQuery();
//				
//				search all document from P8 of DOC_BARCODE. There could be multiple document with the same DOC_BARCODE in P8
//				The search return a sorted set of Documents order by LTST_VER (descending), the the first one is the latest version
//	
				

				if(!rs.next()) { // if there no record for this DOC_GRP_NO, skip it and log an error
					bulkMoveOutputDataFile.write(String.format("No Main Form,%d,,,,\n",doc_grp_no).getBytes());
					continue; 
				}

				ArrayList<String> primaryFormBarcodeArray = new ArrayList<String>(); 
				while("Y".equalsIgnoreCase(rs.getNString("PRMRY_IND"))) {
					primaryFormBarcodeArray.add(rs.getNString("DOC_BARCODE"));
					
					if(!rs.next()) 
						break;

				}
				if(primaryFormBarcodeArray.size()==0) {
					log.error(String.format("%d has no main form", doc_grp_no));
					continue;
				}
				
				if(primaryFormBarcodeArray.size()>1) {
					log.error(String.format("%d more than one main form", doc_grp_no));
					continue;
				}
//				String prmryInd = rs.getNString("PRMRY_IND");
//				if (!"Y".equalsIgnoreCase(prmryInd)) { // if the first record of this doc_grp_no is not main form, skip this doc_grp_no and log this exception
//					log.error(String.format("%d has no main form", doc_grp_no));
//					continue;
//				} 

				
//	
//				get the DOC_BARCODE of main form
//					
				mainFormDocBarcode = primaryFormBarcodeArray.get(0);
				
				// mainFormDocSortedSet contains all versions of the main form
				// of mainFormDocBarcode in reverse order of LTST_VER
				SortedSet<Document> mainFormDocSortedSet; 					
				mainFormDocSortedSet = searchDocByBarcode(mainFormDocBarcode, "ICRIS_Reg_Doc");
				
				Iterator<Document> mainFormDocIt = mainFormDocSortedSet.iterator();
				
				while(mainFormDocIt.hasNext()) { //	 For each version of main form, the last version come first
					
					Document mainFormDoc = mainFormDocIt.next();
//					mainFormDoc.fetchProperties(new String[] {"DocumentTitle","F_DOCNUMBER","F_DOCCLASSNUMBER","F_DOCCLASSNUMBER","F_ENTRYDATE","Id","DOC_BARCODE","DOC_SHT_FRM","FLING_DATE","PRT_VW_IND","F_PAGES","LTST_VER", "MimeType","ContentElements","Annotations","ElementSequenceNumber"});					
					mainFormDoc.fetchProperties(new String[] {"F_DOCNUMBER","F_DOCCLASSNUMBER","F_DOCCLASSNUMBER","F_ENTRYDATE","F_ARCHIVEDATE","F_DELETEDATE","F_DOCFORMAT","F_DOCLOCATION","F_DOCTYPE","F_PAGES","F_RETENTOFFSET",
							"DOC_BARCODE","DOC_SHT_FRM","FLING_DATE","PRT_VW_IND","LTST_VER","FNP_ARCHIVE","PRV_DOCID","STATUS",
							"abandoned","Id","DocumentTitle","MimeType","ContentElements","Annotations","ElementSequenceNumber"});

					Integer version = Integer.parseInt(mainFormDoc.getProperties().getStringValue("LTST_VER"), 10);
					bulkMoveOutputDataFile.write(String.format("Main Form,%d,,%s,%s,%d,%s\n",doc_grp_no, mainFormDocBarcode, version, version, mainFormDoc.get_Id().toString()).getBytes());
					bulkMoveOutputDataFile.flush();
					
					
//					
//					reset rs to point to the second record (skip first one because it is the main form)
//					
					rs.beforeFirst();
					rs.next();
//
//					each document is a version of main form
//					
					ArrayList<Document> docArrayList = new ArrayList<Document>();
					while(rs.next()) {
						String piDocBarcode = rs.getNString("DOC_BARCODE");
						SortedSet<Document> piDocSortedSet =searchDocByBarcode(piDocBarcode, "ICRIS_Reg_Doc");
						Document  piDoc = getPIVersion(piDocSortedSet,version);
						if (piDoc==null)
							bulkMoveOutputDataFile.write(String.format("Barcode not found,%d,%s,%s,,%d\n",doc_grp_no, piDocBarcode, mainFormDocBarcode, version).getBytes());
						else
							docArrayList.add(piDoc);
					}
					if(docArrayList.isEmpty())
						bulkMoveOutputDataFile.write(String.format("Secondary document not found,%d,,%s,,%d\n",doc_grp_no,  mainFormDocBarcode, version).getBytes());

					else 
						createPIDocument(docArrayList, mainFormDoc, doc_grp_no, version);
					
				}
				
				
			} // END of read batch data file, loop through DOC_GRP_NO
			
			reader.close();

			if (isOverdue) {
//				log.info(String.format("overdue,%s/%s,%010.0f,%010.0f,%d",batchSetId,datFileName,this.firstDocNumberMoved,this.lastDocNumberMoved,this.numOfDocMoved));
			} else {
//				log.info(String.format("finished,%s/%s,%010.0f,%010.0f,%d",batchSetId,datFileName,this.firstDocNumberMoved,this.lastDocNumberMoved,this.numOfDocMoved));
			}
	}
	
	public void waitToStart() {
		log.info(String.format("waiting,%s/%s,%s",batchSetId,datFileName,batchStartTime.toString()));
		try {
			java.util.Date curDate = new java.util.Date();
			TimeUnit.MILLISECONDS.sleep(this.batchStartTime.getTime() - curDate.getTime());
		} catch (InterruptedException e) {
		
	}
}
	
	
//	public void examineBulkMoveSweepJobResult() {
//
//	}

	private void loadConfig() throws ICRISException {
		SimpleDateFormat formatter = new SimpleDateFormat("dd-MM-yyyy HH:mm:ss");
		try {
			java.util.Properties props = new java.util.Properties();
			props.load(new FileInputStream("config/batchSetConfig/" + this.batchSetId + ".conf"));
			props.load(new FileInputStream("config/oprdb.conf"));
			this.batchStartTime = formatter.parse(props.getProperty("batchStartTime"));
			this.batchEndTime = formatter.parse(props.getProperty("batchEndTime"));
			this.queryString = props.getProperty("RegBarcodeQuery");
			this.JDBCURL = "jdbc:oracle:thin:@//"+ props.getProperty("OPRServer") + ":" + props.getProperty("OPRPort") + "/" + props.getProperty("OPRDatabase");
			this.dbuser = props.getProperty("OPRUser");
			this.dbpassword = props.getProperty("OPRPassword");
			this.previewOnly = "TRUE".equalsIgnoreCase(props.getProperty("PreviewOnly", "False"))?Boolean.TRUE:Boolean.FALSE;
	
		} catch (IOException e) {
			throw new ICRISException(ICRISException.ExceptionCodeValue.BM_LOAD_BATCH_SET_CONFIG_ERROR,"Load batchSetConfig error : " + "config/batchSetConfig/" + this.batchSetId + ".conf", e);
		} catch (ParseException e) {
			throw new ICRISException(ICRISException.ExceptionCodeValue.BM_LOAD_BATCH_SET_CONFIG_ERROR,"BatchSetConfig time format error ", e);			
		} catch (NumberFormatException e) {
			throw new ICRISException(ICRISException.ExceptionCodeValue.BM_LOAD_BATCH_SET_CONFIG_ERROR,"BatchSetConfig integer format error", e);						
		}
	}
	
	private SortedSet<Document> searchDocByBarcode(String barCode, String docClass) {
		SearchSQL searchSQL = new SearchSQL();
		SearchScope searchScope = new SearchScope(revampedCPEUtil.getObjectStore());
		searchSQL.setFromClauseInitialValue(docClass, null, false);
		searchSQL.setSelectList("DOC_BARCODE, F_DOCNUMBER ,DOC_BARCODE, LTST_VER, Id");
		searchSQL.setMaxRecords(99);
		String whereClause = "DOC_BARCODE"+"='" + barCode+"'";
		searchSQL.setWhereClause(whereClause);

//		
//		sort the document in descending order of LTST_VER
//		
		SortedSet<Document> docSortedSet = new TreeSet<Document>(new Comparator<Document>(){
			public int compare(Document one, Document another) {
				return (Integer.parseInt(another.getProperties().getStringValue("LTST_VER"), 10) - Integer.parseInt(one.getProperties().getStringValue("LTST_VER"), 10)) ;
			}
		});	
		
		RepositoryRowSet rs = searchScope.fetchRows(searchSQL, null, null, null);
		
		@SuppressWarnings("unchecked")
		Iterator<RepositoryRow> it = rs.iterator();
		while(it.hasNext()) {
			RepositoryRow rr = it.next();
			Document doc = Factory.Document.fetchInstance(revampedCPEUtil.getObjectStore(), (Id)rr.getProperties().getObjectValue("Id"), null);
			doc.fetchProperties(new String[] {"F_DOCNUMBER","F_DOCCLASSNUMBER","DOC_BARCODE","Id","F_PAGES","MimeType"});
			docSortedSet.add(doc);
		}
		
		return docSortedSet;
	}
	
	private InputStream updateAnnotContent(InputStream is, String targetId, Integer annotedPageNumber) {
		try {
			DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
			DocumentBuilder builder = factory.newDocumentBuilder();
			org.w3c.dom.Document doc = builder.parse(is);
//			org.w3c.dom.Element element = doc.getDocumentElement();
			org.w3c.dom.NodeList nodes = doc.getElementsByTagName("PropDesc");		
			for(int k=0;k< nodes.getLength();k++){
				org.w3c.dom.Node node = nodes.item(k);
				if(node.getNodeType() == Node.ELEMENT_NODE){
					org.w3c.dom.Element e = (org.w3c.dom.Element)node;
					e.setAttribute("F_ID", targetId);
					e.setAttribute("F_ANNOTATEDID", targetId);
					e.setAttribute("F_PAGENUMBER", Integer.toString(annotedPageNumber));
				}
			}
			ByteArrayOutputStream oStream = new ByteArrayOutputStream();
			Source xmlSource = new DOMSource(doc);
			Result oTarget = new StreamResult(oStream);
			TransformerFactory.newInstance().newTransformer().transform(xmlSource, oTarget);
			InputStream isNew = new ByteArrayInputStream(oStream.toByteArray());
			return isNew;
		} catch (Exception e) {
			return null;
		}
	}
	
	
	@SuppressWarnings("unchecked")
	private void createPIDocument(ArrayList<Document> docArrayList, Document mainFormDoc, Integer doc_grp_no, Integer version) throws IOException, SQLException {
		com.filenet.api.property.Properties mainFormDocProperties = mainFormDoc.getProperties();
		String mainFormDocBarcode = mainFormDocProperties.getStringValue("DOC_BARCODE");
		if (this.previewOnly) {
			Iterator<Document> docIt = docArrayList.iterator();
			while (docIt.hasNext()) {
				Document doc = docIt.next();
	//			doc.refresh(new String[] {"F_DOCNUMBER","F_DOCCLASSNUMBER","Id","DOC_BARCODE","F_PAGES","LTST_VER","MimeType","ContentElements","Annotations"});
				doc.fetchProperties(new String[] {"F_DOCNUMBER","F_DOCCLASSNUMBER","F_DOCCLASSNUMBER","F_ENTRYDATE","F_ARCHIVEDATE","F_DELETEDATE","F_DOCFORMAT","F_DOCLOCATION","F_DOCTYPE","F_PAGES","F_RETENTOFFSET",
						"DOC_BARCODE","DOC_SHT_FRM","FLING_DATE","PRT_VW_IND","LTST_VER","FNP_ARCHIVE","PRV_DOCID","STATUS",
						"abandoned","Id","DocumentTitle","MimeType","ContentElements","Annotations","ElementSequenceNumber"});
				bulkMoveOutputDataFile.write(String.format("Add Secondary Page,%d,%s,%s,%s,%d,%s,(Preview Only)\n",doc_grp_no, doc.getProperties().getStringValue("DOC_BARCODE"), mainFormDocBarcode,  doc.getProperties().getStringValue("LTST_VER"), version, doc.get_Id().toString()).getBytes());
				bulkMoveOutputDataFile.flush();
			}
			bulkMoveOutputDataFile.write(String.format("Create Secondary Document,%d,,%s,%d,%d,,(Preview Only)\n",doc_grp_no, mainFormDocBarcode, version, version).getBytes());
			bulkMoveOutputDataFile.flush();
	
			return;
		}

	
	
//	
//	Create a new document of class ICRIS_Protected_Doc, to which the other documents of the same group number will be copied 
//	
//	
//	
		Document piDoc= Factory.Document.createInstance(revampedCPEUtil.getObjectStore(), "ICRIS_Protected_Doc");		
		com.filenet.api.property.Properties piDocProperties = piDoc.getProperties();
		piDocProperties.putValue("DocumentTitle", mainFormDocProperties.getStringValue("DocumentTitle"));
		piDocProperties.putValue("F_ARCHIVEDATE", mainFormDocProperties.getDateTimeValue("F_ARCHIVEDATE"));
		piDocProperties.putValue("F_DELETEDATE", mainFormDocProperties.getDateTimeValue("F_DELETEDATE"));
		piDocProperties.putValue("F_DOCFORMAT", mainFormDocProperties.getStringValue("F_DOCFORMAT"));
		piDocProperties.putValue("F_DOCLOCATION", mainFormDocProperties.getStringValue("F_DOCLOCATION"));
		piDocProperties.putValue("F_DOCNUMBER", mainFormDocProperties.getFloat64Value("F_DOCNUMBER"));
		piDocProperties.putValue("F_DOCTYPE", mainFormDocProperties.getStringValue("F_DOCTYPE"));
		piDocProperties.putValue("F_ENTRYDATE", mainFormDocProperties.getDateTimeValue("F_ENTRYDATE"));
		piDocProperties.putValue("F_PAGES", mainFormDocProperties.getInteger32Value("F_PAGES"));
		piDocProperties.putValue("F_RETENTOFFSET", mainFormDocProperties.getInteger32Value("F_RETENTOFFSET"));
		piDocProperties.putValue("F_DOCCLASSNUMBER", mainFormDocProperties.getInteger32Value("F_DOCCLASSNUMBER"));

		piDocProperties.putValue("LTST_VER", mainFormDocProperties.getStringValue("LTST_VER"));
		piDocProperties.putValue("DOC_BARCODE", mainFormDocProperties.getStringValue("DOC_BARCODE"));
//		piDocProperties.putValue("DOC_SHT_FRM", mainFormDocProperties.getStringValue("DOC_SHT_FRM"));
//		piDocProperties.putValue("FNP_ARCHIVE", mainFormDocProperties.getDateTimeValue("FNP_ARCHIVE"));
//		piDocProperties.putValue("PRT_VW_IND", mainFormDocProperties.getStringValue("PRT_VW_IND"));
//		piDocProperties.putValue("PRV_DOCID", mainFormDocProperties.getStringValue("PRV_DOCID"));
//		piDocProperties.putValue("STATUS", mainFormDocProperties.getStringValue("STATUS"));
		piDoc.set_MimeType(mainFormDoc.get_MimeType());
		piDoc.save(RefreshMode.REFRESH);	
		
		ContentElementList scondaryDocContentList = Factory.ContentElement.createList();
	
		
		Integer annotatedElementIndex = 1;
//
//		 
//		iterate other documents of the same group number
//		
//
		String mime_type=null;
		Iterator<Document> docIt = docArrayList.iterator();
		while (docIt.hasNext()) {
			Document doc = docIt.next();
//			doc.refresh(new String[] {"F_DOCNUMBER","F_DOCCLASSNUMBER","Id","DOC_BARCODE","F_PAGES","LTST_VER","MimeType","ContentElements","Annotations"});
			doc.refresh(new String[] {"F_DOCNUMBER","F_DOCCLASSNUMBER","F_DOCCLASSNUMBER","F_ENTRYDATE","F_ARCHIVEDATE","F_DELETEDATE","F_DOCFORMAT","F_DOCLOCATION","F_DOCTYPE","F_PAGES","F_RETENTOFFSET",
					"DOC_BARCODE","DOC_SHT_FRM","FLING_DATE","PRT_VW_IND","LTST_VER","FNP_ARCHIVE","PRV_DOCID","STATUS",
					"abandoned","Id","DocumentTitle","MimeType","ContentElements","Annotations","ElementSequenceNumber"});
			mime_type = doc.get_MimeType();
		
			ContentElementList docContentList = doc.get_ContentElements();
			
//
//		
//		
//		Add all content element to the newly created secondary document
//		

			Iterator<ContentTransfer> it = docContentList.iterator();
			while(it.hasNext()) {
				ContentTransfer oldctObject = it.next();
				ContentTransfer  ctObject = Factory.ContentTransfer.createInstance();
				ctObject.setCaptureSource(oldctObject.accessContentStream());
				scondaryDocContentList.add(ctObject);
			}
			
			
//		
//		 Copy annotations from to secondary documents
//
		AnnotationSet originalAnnos = doc.get_Annotations();
		Iterator iter = originalAnnos.iterator();
		while (iter.hasNext()) {
			Annotation originalAnnot = (Annotation)iter.next();
			originalAnnot.fetchProperties(new String[] {"BatchID","Id","DescriptiveText","Permissions","Creator", "DateCreated","AnnotatedContentElement"});
			Annotation annObject = Factory.Annotation.createInstance(revampedCPEUtil.getObjectStore(), "Annotation");
			annObject.set_Permissions(originalAnnot.get_Permissions());
			annObject.set_Creator(originalAnnot.get_Creator());
			annObject.set_DateCreated(originalAnnot.get_DateCreated());
			annObject.getProperties().putValue("BatchID", originalAnnot.get_Id().toString());
			annObject.getProperties().putValue("DescriptiveText", originalAnnot.getProperties().getStringValue("DescriptiveText"));
			annObject.set_AnnotatedContentElement(originalAnnot.get_AnnotatedContentElement() + annotatedElementIndex);
			annObject.set_AnnotatedObject(piDoc);
			annObject.save(RefreshMode.REFRESH);	
			ContentElementList newCels = Factory.ContentElement.createList();
			ContentElementList cels = originalAnnot.get_ContentElements();
			Iterator<ContentElement> ceIter = cels.iterator();
			while (ceIter.hasNext()) {
				ContentElement ce = ceIter.next();
				if(ce instanceof ContentTransfer){
					InputStream is = ((ContentTransfer)ce).accessContentStream();
					ContentTransfer ctNew = Factory.ContentTransfer.createInstance();
					ctNew.setCaptureSource(updateAnnotContent(is, annObject.get_Id().toString(), originalAnnot.get_AnnotatedContentElement() + annotatedElementIndex));
					newCels.add(ctNew);
				}
			}				
			annObject.set_ContentElements(newCels);
			annObject.save(RefreshMode.REFRESH);
			originalAnnot.getProperties().putValue("BatchID", annObject.get_Id().toString());
			originalAnnot.save(RefreshMode.REFRESH);
			
		}
			
			
			doc.getProperties().putObjectValue("abandoned", true);
			doc.save(RefreshMode.REFRESH);
			annotatedElementIndex += docContentList.size();
			bulkMoveOutputDataFile.write(String.format("Add Secondary Page,%d,%s,%s,%s,%d,%s\n",doc_grp_no, doc.getProperties().getStringValue("DOC_BARCODE"), mainFormDocBarcode,  doc.getProperties().getStringValue("LTST_VER"), version, doc.get_Id().toString()).getBytes());
			bulkMoveOutputDataFile.flush();
		}
				
		piDoc.set_ContentElements(scondaryDocContentList);
		piDoc.set_MimeType(mime_type);
		piDoc.checkin(AutoClassify.DO_NOT_AUTO_CLASSIFY, CheckinType.MAJOR_VERSION);
		piDoc.save(RefreshMode.REFRESH);
		bulkMoveOutputDataFile.write(String.format("Create Secondary Document,%d,,%s,%d,%d,%s\n",doc_grp_no, mainFormDocBarcode, version, version, piDoc.get_Id().toString()).getBytes());
		bulkMoveOutputDataFile.flush();

		
	}
	
	
	private Document getPIVersion(SortedSet<Document> piDocSortedSet,Integer version) {
		Iterator<Document> docIt = piDocSortedSet.iterator();
		Integer piVersion;
		while(docIt.hasNext()) { // find a piDoc of which LTST_VER match version
			Document piDoc= docIt.next();
			piVersion = Integer.parseInt(piDoc.getProperties().getStringValue("LTST_VER"), 10);
			if(piVersion==version) return piDoc; 
		}
		
		return piDocSortedSet.isEmpty() ? null:piDocSortedSet.first();
//		if (piDocSortedSet.isEmpty())
//			return null;
//		else return piDocSortedSet.first(); // if not match , return the max version in piDocSortedSet
	}
}


