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

public class BulkMoverManager_PI_Pend implements Runnable {
	private String batchSetId;
	private CPEUtil revampedCPEUtil;
	private CSVParser csvParser;
	private ICRISLogger log;
	private java.util.Date batchStartTime;
	private java.util. Date batchEndTime;
	private Boolean previewOnly;
	private String datFileName;
	private FileOutputStream bulkMoveOutputDataFile;
//	private HashMap<Integer, String> classNumToSymNameMap = new HashMap<Integer, String>();
	private Double firstDocNumberMoved=0.0;
	private Integer numOfDocMoved = 0;
	
	private String JDBCURL;
	private String dbuser;
	private String dbpassword;
	private String queryString;

	private Double lastDocNumberMoved = 0.0;
	private java.sql.Connection conn = null;
	
	
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
	
	public BulkMoverManager_PI_Pend(String batchSetId, String datFileName, ICRISLogger log, FileOutputStream ofs,CPEUtil revampedCPEUtil) {
		// TODO Auto-generated constructor stub
		
		this.log = log;
		this.batchSetId = batchSetId;
		this.datFileName = datFileName;
		this.csvParser = new CSVParser();
		this.bulkMoveOutputDataFile = ofs;

		try {
			java.util.Properties props = new java.util.Properties();
			props.load(new FileInputStream("config/batchSetConfig/" + this.batchSetId + ".conf"));
			this.revampedCPEUtil = revampedCPEUtil;
			loadBatchSetConfig();;
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
		Integer pages = 0;
		BufferedReader reader;
 		
			this.numOfDocMoved = 0;

//
//			read batch data file, each line contains a DOC_BARCODE
//
			reader = new BufferedReader(new FileReader("data/bulkMoveBatches_PI_Pend/batchSets/" + this.batchSetId +"/" + this.datFileName));
			for (String line; (line = reader.readLine()) != null; ){
//				
//				check batch end time
//				
				java.util.Date date = new java.util.Date();
				if (date.after(batchEndTime)) { // if batch end time is reached, end the batch
					isOverdue = true;
					break;
				}
				
				String[] parsedLine = csvParser.lineParser(line);
				String doc_barcode = parsedLine[0];
//				System.out.println("barcode :" + doc_barcode);
//				
//				search all records in CR_SCAN_DOC_PG_RLTSHP table for records by DOC_BARCODE ORDER BY PG_NO ASC
//				
				PreparedStatement queryStatement = this.conn.prepareStatement(this.queryString, ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_READ_ONLY);
				queryStatement.setNString(1, doc_barcode);
				
//			
//				the result set contains all pages of the same DOC_BARCODE and order by PG_NO
//				each row of the result set represent one content element (i.e. one page) of P8 Document. Its PG_NO is the page number of this content element in the document
//				Note that PG_NO start from 1, while content element sequence number start from 0;
//				
				ResultSet rs = queryStatement.executeQuery();
				
			
//
//				
//				annotESNMap is an array. The i-th element contains information of the i-th page of the document. 
//				the information includes an indicator to indicate whether this page is PI or not
//				and the element sequence number (ESN) of this page in the Protected Document or Pend Document
				
				
				if(!rs.last()) { // move cursor to the last, if false , there no rows in the result set
//					log.error(String.format("DOC_BARCODE=%s is not found in CR_SCAN_DOC_PG_RLTSH table", doc_barcode));
					bulkMoveOutputDataFile.write(String.format("Barcode Not Found in OPR Database,%s,,ICRIS_Pend_Doc\n",doc_barcode).getBytes());
					continue;					
				}
				Integer maxPageNo =  rs.getInt("PG_NO"); // the value of PG_NO of the last row is the largest page no
				
				ArrayList<HashMap<String, Object>> annotESNMap = new ArrayList<HashMap<String, Object>>(); // create an ArrayList with size equal to maxPageNo, initialize all element to null
				for (Integer i=0; i< maxPageNo; i++) {
					annotESNMap.add(null);
				}
				
				Integer protectedDocESN = 0, pendDocESN = 0;
				
				rs.beforeFirst(); // reset cursor to the begining
				while (rs.next()) {
//					System.out.printf("%s, %d, %s,", doc_barcode, rs.getInt("PG_NO"),rs.getNString("PI_IND"));
					Integer pageNo = rs.getInt("PG_NO");
					HashMap<String, Object> temp = new HashMap<String, Object>();
					if ("Y".equalsIgnoreCase(rs.getNString("PI_IND"))) {
						temp.put("annotESN", protectedDocESN++);
						temp.put("isPI", Boolean.TRUE);						

					} else {
						temp.put("annotESN", pendDocESN++);
						temp.put("isPI", Boolean.FALSE);				
					}
					annotESNMap.add(pageNo -1 ,temp);
				}				
				rs.close();

				// 
				// Search all Pending Document in P8 ObjectStore with the same DOC_BARCODE 
				// each returned document is a version of the same DOC_BARCODE
				//
				SortedSet<Document> docSortedSet = searchDocByBarcode(doc_barcode, "ICRIS_Pend_Doc", Boolean.FALSE);
				
				if (docSortedSet.size() == 0) { // If there are no pending document of this DOC_BARCODE have been imported to P8, skip it
//					log.error(String.format("DOC_BARCODE=%s is not found in ObjectStore", doc_barcode));
					bulkMoveOutputDataFile.write(String.format("Barcode Not Found in Object Store,%s,,ICRIS_Pend_Doc\n",doc_barcode).getBytes());
					continue;
				}
				
				Iterator<Document> itDoc = docSortedSet.iterator();
				
				while(itDoc.hasNext()) { // split each version into two documents, according to annotESNMap. The first one is protected document, the second one is not
//					System.out.println("split document");
					Document doc = itDoc.next();
					com.filenet.api.property.Properties docProperties = doc.getProperties();
					bulkMoveOutputDataFile.write(String.format("Split Document,%s,%s,ICRIS_Pend_Doc,%s\n",docProperties.getStringValue("DOC_BARCODE"), docProperties.getStringValue("LTST_VER"), doc.get_Id().toString()).getBytes());
					splitPendDoc(doc, annotESNMap);					
				}

					
				
			} // END of read batch data file
			
			reader.close();

			if (isOverdue) {
				log.info(String.format("overdue,%s/%s",batchSetId,datFileName));
			} else {
				log.info(String.format("finished,%s/%s",batchSetId,datFileName));
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
	

	private void loadBatchSetConfig() throws ICRISException {
		SimpleDateFormat formatter = new SimpleDateFormat("dd-MM-yyyy HH:mm:ss");
		try {			
			java.util.Properties props = new java.util.Properties();
			props.load(new FileInputStream("config/batchSetConfig/" + this.batchSetId + ".conf"));
			props.load(new FileInputStream("config/oprdb.conf"));
			this.batchStartTime = formatter.parse(props.getProperty("batchStartTime"));
			this.batchEndTime = formatter.parse(props.getProperty("batchEndTime"));
			this.queryString = props.getProperty("PendBarcodeQuery");
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
	
	private SortedSet<Document> searchDocByBarcode(String barCode, String docClass, Boolean abandoned) {
		SearchSQL searchSQL = new SearchSQL();
		SearchScope searchScope = new SearchScope(revampedCPEUtil.getObjectStore());
		searchSQL.setFromClauseInitialValue(docClass, null, true);
		searchSQL.setSelectList("DOC_BARCODE, F_DOCNUMBER ,DOC_BARCODE, LTST_VER, Id, abandoned");
		searchSQL.setMaxRecords(99);
		String whereClause = "DOC_BARCODE"+"='" + barCode+"'"/* + " AND abandoned=" + abandoned.toString()*/;
		searchSQL.setWhereClause(whereClause);

		
		SortedSet<Document> docSortedSet = new TreeSet<Document>(new Comparator<Document>(){
			public int compare(Document one, Document another) {
				return (Integer.parseInt(another.getProperties().getStringValue("LTST_VER"), 10) - Integer.parseInt(one.getProperties().getStringValue("LTST_VER"), 10)) ;
			}
		});	
		
		RepositoryRowSet rs = searchScope.fetchRows(searchSQL, null, null, null);
		
		Iterator<RepositoryRow> it = rs.iterator();
		while(it.hasNext()) {
			RepositoryRow rr = it.next();
			if(rr.getProperties().getBooleanValue("abandoned") || isPIExist(barCode, rr.getProperties().getStringValue("LTST_VER"))) continue; // if this document is abandoned or it has protected document, skip it.
			Document secondaryDoc = Factory.Document.fetchInstance(revampedCPEUtil.getObjectStore(), (Id)rr.getProperties().getObjectValue("Id"), null);
			docSortedSet.add(secondaryDoc);
		}
		
		return docSortedSet;
	}

	private Boolean isPIExist(String barCode, String sVer) {
		SearchSQL searchSQL = new SearchSQL();
		SearchScope searchScope = new SearchScope(revampedCPEUtil.getObjectStore());
		searchSQL.setFromClauseInitialValue("ICRIS_Protected_Doc", null, true);
		searchSQL.setSelectList("DOC_BARCODE, F_DOCNUMBER ,DOC_BARCODE, LTST_VER, Id, abandoned");
		searchSQL.setMaxRecords(99);
		String whereClause = "DOC_BARCODE"+"='" + barCode+"'" + " AND LTST_VER=" + sVer.toString();
		searchSQL.setWhereClause(whereClause);
		
		RepositoryRowSet rs = searchScope.fetchRows(searchSQL, null, null, null);		
		Iterator<RepositoryRow> it = rs.iterator();
		
		return it.hasNext();
	}
	
	
	private InputStream updateAnnotContent(InputStream is, String targetId,ArrayList<HashMap<String, Object>> annotESNMap) {
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
//					e.setAttribute("F_PAGENUMBER", Integer.toString(annotedPageNumber));
					Integer orignialPageNo = Integer.parseInt(e.getAttribute("F_PAGENUMBER"), 10);
					e.setAttribute("F_PAGENUMBER",String.valueOf((Integer)annotESNMap.get(orignialPageNo - 1).get("annotESN") + 1));
;
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
	
	
	private void splitPendDoc(Document doc,ArrayList<HashMap<String, Object>> annotESNMap) throws SQLException, IOException {

//		
//		create two empty content element list. one is used to store the content  elements of protected document
//		the other one is used to store the content elements of non protected document 
//		
//		
		ContentElementList protectedDocCEL = Factory.ContentElement.createList();
		ContentElementList pendDocCEL = Factory.ContentElement.createList();
		
		ContentElementList docContentList = doc.get_ContentElements();
		Iterator<ContentTransfer> itCt = docContentList.iterator();
		
		while(itCt.hasNext()) {
			ContentTransfer ct = itCt.next(); 
			Integer ESN = ct.get_ElementSequenceNumber();
			if (ESN > annotESNMap.size() - 1) { // if the ESN of the content element beyond the range of annotESNMap, assume this content element is non-protected
				pendDocCEL.add(ct);
				this.log.info(String.format("DOC_BARCODE=%s, LTST_VER=%s ESN=%d Content ESN beyond the range in CR_SCAN_DOC_PG_RLTSHP", doc.getProperties().getStringValue("DOC_BARCODE"), doc.getProperties().getStringValue("LTST_VER"), ESN));
			} else {
				HashMap<String, Object> annotESNItem = annotESNMap.get(ESN);
				if (annotESNItem != null) {
					if((Boolean)annotESNMap.get(ESN).get("isPI")) {
						protectedDocCEL.add(ct);
					} else {
						pendDocCEL.add(ct);
					}				
				} else { // if this entry of annotESNMap is null, it means that this ESN does not exists in CR_SCAN_DOC_PG_RLTSHP, assume the corresponding page is non-protected
					pendDocCEL.add(ct);
					this.log.info(String.format("DOC_BARCODE=%s, LTST_VER=%s ESN=%d Content ESN is not found in CR_SCAN_DOC_PG_RLTSHP", doc.getProperties().getStringValue("DOC_BARCODE"), doc.getProperties().getStringValue("LTST_VER"), ESN));
				}
			}

		}
		
		if(protectedDocCEL.size()!=0) {
			if (!this.previewOnly){			
				doc.fetchProperties(new String[] {"F_DOCNUMBER","F_DOCCLASSNUMBER","F_DOCCLASSNUMBER","F_ENTRYDATE","F_ARCHIVEDATE","F_DELETEDATE","F_DOCFORMAT","F_DOCLOCATION","F_DOCTYPE","F_PAGES","F_RETENTOFFSET",
						"DOC_BARCODE","DOC_SHT_FRM","FLING_DATE","PRT_VW_IND","LTST_VER","FNP_ARCHIVE","PRV_DOCID","STATUS",
						"abandoned","Id","DocumentTitle","MimeType","ContentElements","Annotations","ElementSequenceNumber"});			
	//
	//		split the content elements into two documents
	//		

				Document protectedDoc = createDoc(protectedDocCEL, doc, "ICRIS_Protected_Doc");
				Document pendDoc = createDoc(pendDocCEL, doc, "ICRIS_Pend_Doc");
				doc.getProperties().putValue("abandoned", Boolean.TRUE);
				doc.save(RefreshMode.REFRESH);
		
	//		
	//		 Copy annotations to protectedDoc and pendDoc
	//
	//		Integer protectedDocAnnotElementIndex = 0, pendDocAnnotElementIndex = 0;
				
				AnnotationSet originalAnnos = doc.get_Annotations();
				Iterator<Annotation> itAnnot = originalAnnos.iterator();
				while (itAnnot.hasNext()) {
					Annotation originalAnnot = itAnnot.next();
					originalAnnot.fetchProperties(new String[] {"BatchID","Id","DescriptiveText","Permissions","Creator", "DateCreated","AnnotatedContentElement", "Permissions"});
					Annotation annObject = Factory.Annotation.createInstance(revampedCPEUtil.getObjectStore(), "Annotation");
					annObject.set_Creator(originalAnnot.get_Creator());
					annObject.set_DateCreated(originalAnnot.get_DateCreated());
					annObject.getProperties().putValue("DescriptiveText", originalAnnot.getProperties().getStringValue("DescriptiveText"));
					annObject.set_AnnotatedContentElement((Integer)annotESNMap.get(originalAnnot.get_AnnotatedContentElement()).get("annotESN"));
					
					Integer annotedPageNo = getAnnotedPageNo((ContentTransfer)originalAnnot.get_ContentElements().get(0));
					if((Boolean)annotESNMap.get(annotedPageNo - 1).get("isPI")){ //
						annObject.set_AnnotatedObject(protectedDoc);
//						System.out.printf("isPI\n");
					} else {
						annObject.set_AnnotatedObject(pendDoc);
//						System.out.printf("isNotPI\n");
					}
					annObject.save(RefreshMode.REFRESH);	
					ContentElementList newCels = Factory.ContentElement.createList();
					ContentElementList cels = originalAnnot.get_ContentElements();
					Iterator<ContentTransfer> ceIter = cels.iterator();
					while (ceIter.hasNext()) {
						ContentTransfer ct = ceIter.next();
						if(ct instanceof ContentTransfer){
							InputStream is = ct.accessContentStream();
							ContentTransfer ctNew = Factory.ContentTransfer.createInstance();
		//					ctNew.setCaptureSource(updateAnnotContent(is, annObject.get_Id().toString(), (Integer)annotESNMap.get(getAnnotedPageNo(ct) - 1).get("annotESN") + 1));
							ctNew.setCaptureSource(updateAnnotContent(is, annObject.get_Id().toString(), annotESNMap));
							newCels.add(ctNew);
						}
					}				
					annObject.set_ContentElements(newCels);
					annObject.save(RefreshMode.REFRESH);
					originalAnnot.getProperties().putValue("BatchID", annObject.get_Id().toString());
					originalAnnot.save(RefreshMode.REFRESH);			
				}			
			} else {
				String doc_barcode = doc.getProperties().getStringValue("DOC_BARCODE");
				String version = doc.getProperties().getStringValue("LTST_VER");
				doc.refresh(new String[] {"F_DOCNUMBER","F_DOCCLASSNUMBER","Id","DOC_BARCODE","F_PAGES","LTST_VER","MimeType","ContentElements","Annotations"});
				bulkMoveOutputDataFile.write(String.format("%s,%s,%s,%s(Preview Only)\n",doc_barcode, version, "ICRIS_Protected_Doc", "{********-****-****-****-********}").getBytes());
				bulkMoveOutputDataFile.write(String.format("%s,%s,%s,%s(Preview Only)\n",doc_barcode, version, "ICRIS_Pend_Doc", "{********-****-****-****-********}").getBytes());
				bulkMoveOutputDataFile.flush();				
			}
		} else {
			bulkMoveOutputDataFile.write(String.format("No Protected Pages,%s,%s,ICRIS_Pend_Doc)\n",doc.getProperties().getStringValue("DOC_BARCODE"), doc.getProperties().getStringValue("LTST_VER")).getBytes());
//			log.info(String.format("DOC_BARCODE=%s, LTST_VER=%s does not have protected pages", doc.getProperties().getStringValue("DOC_BARCODE"), doc.getProperties().getStringValue("LTST_VER")));
		}
		
	}


	private Document createDoc(ContentElementList cel, Document doc, String className) throws IOException {

		if (cel.size()==0) return null;
		com.filenet.api.property.Properties docProperties = doc.getProperties();

		Document  newDoc = Factory.Document.createInstance(revampedCPEUtil.getObjectStore(), className);
		com.filenet.api.property.Properties newDocProperties = newDoc.getProperties();
		newDocProperties.putValue("DocumentTitle", docProperties.getStringValue("DocumentTitle"));

		newDocProperties.putValue("F_ARCHIVEDATE", docProperties.getDateTimeValue("F_ARCHIVEDATE"));
		newDocProperties.putValue("F_DELETEDATE", docProperties.getDateTimeValue("F_DELETEDATE"));
		newDocProperties.putValue("F_DOCFORMAT", docProperties.getStringValue("F_DOCFORMAT"));
		newDocProperties.putValue("F_DOCLOCATION", docProperties.getStringValue("F_DOCLOCATION"));
		newDocProperties.putValue("F_DOCNUMBER", docProperties.getFloat64Value("F_DOCNUMBER"));
		newDocProperties.putValue("F_DOCTYPE", docProperties.getStringValue("F_DOCTYPE"));
		newDocProperties.putValue("F_ENTRYDATE", docProperties.getDateTimeValue("F_ENTRYDATE"));
		newDocProperties.putValue("F_PAGES", docProperties.getInteger32Value("F_PAGES"));
		newDocProperties.putValue("F_RETENTOFFSET", docProperties.getInteger32Value("F_RETENTOFFSET"));
		newDocProperties.putValue("F_DOCCLASSNUMBER", docProperties.getInteger32Value("F_DOCCLASSNUMBER"));
		newDocProperties.putValue("LTST_VER", docProperties.getStringValue("LTST_VER"));

		newDocProperties.putValue("LTST_VER", docProperties.getStringValue("LTST_VER"));
		newDocProperties.putValue("DOC_BARCODE", docProperties.getStringValue("DOC_BARCODE"));
		
		if("ICRIS_Pend_Doc".equalsIgnoreCase(className)) {
			newDocProperties.putValue("DOC_SHT_FRM", docProperties.getStringValue("DOC_SHT_FRM"));
			newDocProperties.putValue("FNP_ARCHIVE", docProperties.getDateTimeValue("FNP_ARCHIVE"));
			newDocProperties.putValue("PRT_VW_IND", docProperties.getStringValue("PRT_VW_IND"));
			newDocProperties.putValue("PRV_DOCID", docProperties.getStringValue("PRV_DOCID"));
			newDocProperties.putValue("STATUS", docProperties.getStringValue("STATUS"));
		}

		
		newDoc.set_MimeType(doc.get_MimeType());
		newDoc.save(RefreshMode.REFRESH);		
		ContentElementList newCEL = Factory.ContentElement.createList();
		//
		
		Iterator it = cel.iterator();
		while(it.hasNext()) {
			ContentTransfer oldctObject = (ContentTransfer)it.next();
			ContentTransfer  ctObject = Factory.ContentTransfer.createInstance();
			ctObject.setCaptureSource(oldctObject.accessContentStream());
//			ctObject.set_RetrievalName(oldctObject.get_RetrievalName());
//			ctObject.set_ContentType(oldctObject.get_ContentType());
			newCEL.add(ctObject);
		}
		

			newDoc.set_ContentElements(newCEL);
			newDoc.checkin(AutoClassify.DO_NOT_AUTO_CLASSIFY, CheckinType.MAJOR_VERSION);
			newDoc.save(RefreshMode.REFRESH);
			bulkMoveOutputDataFile.write(String.format("%s,%s,%s,%s,%s(Document Created)\n",docProperties.getStringValue("DOC_BARCODE"), docProperties.getStringValue("LTST_VER"), className, newDoc.get_Id().toString(), newDoc.get_Id().toString()).getBytes());
			bulkMoveOutputDataFile.flush();

		return newDoc;
	}
	
	private Integer getAnnotedPageNo(ContentTransfer ct) {
//		System.out.println(ct.get_RetrievalName());
		Integer annotedPageNo =0;
		try {
			DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
			DocumentBuilder builder = factory.newDocumentBuilder();
			org.w3c.dom.Document doc = builder.parse(ct.accessContentStream());
			org.w3c.dom.Element element = doc.getDocumentElement();
			org.w3c.dom.NodeList nodes = doc.getElementsByTagName("PropDesc");		
			org.w3c.dom.Node node = nodes.item(0);
			if(node.getNodeType() == Node.ELEMENT_NODE){
				org.w3c.dom.Element e = (org.w3c.dom.Element)node;
				annotedPageNo = Integer.parseInt(e.getAttribute("F_PAGENUMBER"),10);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return annotedPageNo;
	}
}


