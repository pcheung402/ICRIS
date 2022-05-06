package com.icris.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.FileNotFoundException;
import java.util.*;
import java.nio.file.Paths;
import javax.security.auth.Subject;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Result;
import javax.xml.transform.Source;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.util.concurrent.*;


import com.filenet.api.collection.*;
import com.filenet.api.core.Annotation;
import com.filenet.api.core.Connection;
import com.filenet.api.core.ContentTransfer;
import com.filenet.api.core.Domain;
import com.filenet.api.core.Factory;
import com.filenet.api.core.ObjectStore;
import com.filenet.api.core.Document;
import com.filenet.api.exception.EngineRuntimeException;
import com.filenet.api.exception.ExceptionCode;
import com.filenet.api.util.UserContext;
import com.filenet.api.admin.*;
import com.filenet.api.constants.*;
import com.filenet.api.replication.*;


public class CPEUtil {
	private String CONF_FILE_DIR;
	private ICRISLogger log;
	private final String CPEDomain = "CPEDomain";
	private final String CPEServer="CPEServer";
	private final String CPEPort="CPEPort";
	private final String CPEUser="CPEUser";
	private final String CPEPassword="CPEPassword";
	
	private Connection con;
	private Domain dom;
	private ObjectStoreSet ost;
	private ObjectStore os = null;
	private StoragePolicySet sps;
	private StoragePolicy sp = null;
	private StorageAreaSet sas;
	private FileStorageArea sa = null;
	private boolean isConnected;
	private UserContext uc;
	private Subject sub;
	private Integer CFS_IS_Retries;
	private DocumentBuilderFactory factory;
	private DocumentBuilder builder;
	private Big5ToUTFMapper big5ToUTF16Mapper;
	private Semaphore semFileStore;
	
	public CPEUtil(String configFile,ICRISLogger log) throws ICRISException, ParserConfigurationException{
		this.log = log;
		CONF_FILE_DIR = "." + File.separator + "config" + File.separator + configFile;
		System.out.println("configure file : " + CONF_FILE_DIR);
		try {
			semFileStore = new Semaphore(1, true);
			factory = DocumentBuilderFactory.newInstance();
			 builder = factory.newDocumentBuilder();
			big5ToUTF16Mapper = new Big5ToUTFMapper("Big5-HKSCS", "UTF-16BE");
			InputStream input = new FileInputStream(CONF_FILE_DIR);
	        Properties props = new Properties();
	        // load a properties file
	        props.load(input);
	        CFS_IS_Retries = Integer.valueOf(props.getProperty("CFS_IS_Retries"));
			String userName = props.getProperty(CPEUser);
			String password = props.getProperty(CPEPassword);
			String protocol = props.getProperty("Protocol", "http");
			String uri = protocol + "://" + props.getProperty(CPEServer)+":" +props.getProperty(CPEPort) +"/wsi/FNCEWS40MTOM";
			System.out.println(uri);
//			System.out.println(userName + "/" + password);
	        con = Factory.Connection.getConnection(uri);
	        this.sub = UserContext.createSubject(con,userName,password,"FileNetP8");
	        UserContext.get().pushSubject(this.sub);
	        dom = Factory.Domain.fetchInstance(con, null, null);
	        ost = dom.get_ObjectStores();
	        Iterator<ObjectStore> itos=ost.iterator();
	        while (itos.hasNext()) {
	        	ObjectStore temp = itos.next();
	        	if (temp.get_Name().equals(props.getProperty("CPEOS"))) {
	        		this.os=temp;
	        		break;
	        	}
	        }
        	if(this.os==null) {
        		throw new ICRISException(ICRISException.ExceptionCodeValue.CPE_INVALID_OS_NAME, "Invalid Objec Store Name in File " + CONF_FILE_DIR);
        	}
        	
        if (!props.getProperty("CPESP").equals("*")) {	
	        	this.sps = this.os.get_StoragePolicies();
	        	Iterator<StoragePolicy> itsp=sps.iterator();
	        	while (itsp.hasNext()) {
	        		StoragePolicy temp = itsp.next();
//	    			System.out.println(temp.get_DisplayName());
	        		if (temp.get_DisplayName().equals(props.getProperty("CPESP"))) {
//	        			System.out.println("Storage Policy found : " + temp.get_DisplayName());
	        			this.sp = temp;
	        			break;
	        		}
	        	}
	        	
	        	if (this.sp==null) {
	        		throw new ICRISException(ICRISException.ExceptionCodeValue.CPE_INVALID_SP_NAME, "Invalid Storage Policy Name in File " + CONF_FILE_DIR);        		
	        	}
	        	
	    		/*
	    		 *  Sort all OPEN storage area by CmStandbyActivationPriority
	    		 */
	    		SortedSet<FileStorageArea> saSortedSet = new TreeSet<FileStorageArea>(new Comparator<FileStorageArea>(){
	    			public int compare(FileStorageArea one, FileStorageArea another) {
	    				return one.get_CmStandbyActivationPriority() - another.get_CmStandbyActivationPriority() ;
	    			}
	    		});
	        	sas = sp.get_StorageAreas();
				Iterator<FileStorageArea> itsa = sas.iterator();
				while (itsa.hasNext()) {
					FileStorageArea temp = itsa.next();
					temp.fetchProperties(new String[] {"ResourceStatus"});
					if(temp.get_ResourceStatus().equals(ResourceStatus.OPEN) ) {
						saSortedSet.add(temp);
					}
				}				
				/*
				 *  select the first STANDBY or OPEN storage area, if any
				 */
				if (!saSortedSet.isEmpty()) {
					this.sa = saSortedSet.first();
					return;
				} else {
					this.sa = null;
					throw new ICRISException(ICRISException.ExceptionCodeValue.CPE_SA_UNAVAILABLE, "No more standby Storage Area available"); 
				}
			}
	        isConnected = true;					
		} catch (FileNotFoundException e) {
			throw new ICRISException(ICRISException.ExceptionCodeValue.CPE_CONFIG_FILE_NOT_FOUND, "File " + CONF_FILE_DIR + " not found", e);
		} catch (IOException e) {
			throw new ICRISException(ICRISException.ExceptionCodeValue.CPE_CONFIG_FILE_CANNOT_BE_OPENED, "Error in opening/reading" + configFile + " file", e);
		} catch (IllegalArgumentException e) {
			throw new ICRISException(ICRISException.ExceptionCodeValue.CPE_CONFIG_FILE_ILLEGAL_ARGUMENT, "Illegal argument in " + configFile, e);
		} catch (EngineRuntimeException e) {
			if (e.getExceptionCode().equals(ExceptionCode.E_NOT_AUTHENTICATED)) {
				throw new ICRISException(ICRISException.ExceptionCodeValue.CPE_USNAME_PASSWORD_INVALID, "userName or password is invalid", e);				
			} else if (e.getExceptionCode().equals(ExceptionCode.API_INVALID_URI)) {
				throw new ICRISException(ICRISException.ExceptionCodeValue.CPE_URI_INVALID, "userName or password is invalid", e);				
			}
			else {
				throw e;
			}
		}
		
	}
	public Connection getConnection() {
		return this.con;
	}
	
	public Domain getDomain() {
		return dom;
	}
	
	public ObjectStore getObjectStore() {
		return this.os;
	}
	
	public StoragePolicy getStoragePolicy(/*String storagePolicyName*/) {
    	return this.sp;
	}
	
	public Subject getSubject() {
		return this.sub;
	}
	
	public Double  getStorageAreaSize() {
		this.sa.refresh(new String[] {"ContentElementKBytes"});
		return this.sa.get_ContentElementKBytes();
	}
	
	public Integer getCFSISRetries() {
		return this.CFS_IS_Retries;
	}
	

	public Boolean moveContent(Document doc) throws EngineRuntimeException, ICRISException {
		try {
			this.sa.fetchProperties(new String[] {"Id","DisplayName"});
			doc.moveContent(this.sa);
			doc.save(RefreshMode.NO_REFRESH);
			return Boolean.TRUE;
		} catch (EngineRuntimeException e) {
			if (e.getExceptionCode().equals(ExceptionCode.CONTENT_FCA_SAVE_FAILED)) {
				log.info("CONTENT_FCA_SAVE_FAILED" + "," + this.sa.get_DisplayName());
				getNextStorageArea();
				doc.refresh(new String[] {"StorageArea", "NAME"});
				return Boolean.FALSE; /* content move fail, retry, switch to next storage area and retry */
			}
			
			if (e.getExceptionCode().equals(ExceptionCode.CONTENT_SA_STORAGE_AREA_NOT_OPEN)) {
				log.info("CONTENT_SA_STORAGE_AREA_NOT_OPEN"+ "," + this.sa.get_DisplayName());
				this.sa.set_ResourceStatus(ResourceStatus.OPEN);
				this.sa.save(RefreshMode.REFRESH);
				return Boolean.FALSE; /* content move fail, retry, switch to next storage area and retry */
			}
			throw e; /* unhandled exception */
		}
	}
	
	private void getNextStorageArea() throws ICRISException{
		try {
			semFileStore.acquire();
			this.sa.set_ResourceStatus(ResourceStatus.FULL); /* change the current storage area to FULL */
			this.sa.save(RefreshMode.REFRESH);
			log.info(String.format("%s is FULL", sa.get_DisplayName()).toString());
			Integer saPri = this.sa.get_CmStandbyActivationPriority();
			StoragePolicy sp = getStoragePolicy();
			StorageAreaSet sas = sp.get_StorageAreas();
			
			/*
			 *  Sort all STANDBY or OPEN storage area by CmStandbyActivationPriority
			 */
			SortedSet<FileStorageArea> saSortedSet = new TreeSet<FileStorageArea>(new Comparator<FileStorageArea>(){
				public int compare(FileStorageArea one, FileStorageArea another) {
					return one.get_CmStandbyActivationPriority() - another.get_CmStandbyActivationPriority() ;
				}
			});
			Iterator<FileStorageArea> itsa = sas.iterator();
			while (itsa.hasNext()) {
				FileStorageArea temp = itsa.next();
				temp.fetchProperties(new String[] {"ResourceStatus"});
				if(temp.get_ResourceStatus().equals(ResourceStatus.STANDBY)||temp.get_ResourceStatus().equals(ResourceStatus.OPEN) ) {
					saSortedSet.add(temp);
				}
			}
			
			/*
			 *  select the first STANDBY or OPEN storage area, if any
			 */
			if (!saSortedSet.isEmpty()) {
				this.sa = saSortedSet.first();
				this.sa.setUpdateSequenceNumber(null);
				this.sa.set_ResourceStatus(ResourceStatus.OPEN);
				this.sa.save(RefreshMode.REFRESH);
				this.sa.refresh();
				return;
			} else {
				this.sa = null;
				throw new ICRISException(ICRISException.ExceptionCodeValue.CPE_SA_UNAVAILABLE, "No more standby Storage Area available"); 
			}	
		} catch (IllegalArgumentException | InterruptedException e) {
			throw new ICRISException(ICRISException.ExceptionCodeValue.BM_FILESSTORE_SEM_ARBIRATION_ERROR, "FileStore Semaphore Arbitration Error");
		} finally {
			semFileStore.release();
		}
	}

	public void updateAnnot(Document doc) {
		ReplicationGroup rg = doc.get_ReplicationGroup();
		try {			
//			if(rg!=null) {
				log.info(String.format("clear replication group of document - %s", doc.get_Name()));
				doc.set_ReplicationGroup(null);
				doc.save(RefreshMode.REFRESH);
				System.out.println("clear application group");
//			}
			AnnotationSet annotSet = doc.get_Annotations();
			ArrayList<Annotation> annotationSetStaging = new ArrayList<Annotation>(); 
			Iterator<Annotation> annotIt = annotSet.iterator();
			while(annotIt.hasNext()) {
				Annotation annot = annotIt.next();
				annotationSetStaging.add(annot);
			}
			
			Iterator<Annotation> annotItStagging = annotationSetStaging.iterator();
			while(annotItStagging.hasNext()) {
				Annotation annotOrig = annotItStagging.next();
				Annotation annotNew = Factory.Annotation.createInstance(os, "Annotation");
				ContentElementList cel = annotOrig.get_ContentElements();
				annotNew.set_AnnotatedObject(doc);
				annotNew.set_AnnotatedContentElement(annotOrig.get_AnnotatedContentElement());
				annotNew.setUpdateSequenceNumber(annotOrig.getUpdateSequenceNumber());
				annotNew.set_Permissions(annotOrig.get_Permissions());
				annotNew.save(RefreshMode.REFRESH);
				String annotId = annotNew.get_Id().toString();
//				System.out.println(annotId);
				
				ContentElementList celNew = Factory.ContentElement.createList();
				Iterator<ContentTransfer> ctIt = cel.iterator();
				while (ctIt.hasNext()) {
					ContentTransfer ctNew = Factory.ContentTransfer.createInstance();
					ContentTransfer ctOrig = ctIt.next();
					InputStream is = ctOrig.accessContentStream();
					is = updateAnnotGUID(is, annotId);
					is = updateAnnotTEXT(is);
					ctNew.setCaptureSource(is);
					ctNew.set_ContentType(ctOrig.get_ContentType());
					ctNew.set_RetrievalName(ctOrig.get_RetrievalName());
					celNew.add(ctNew);
				}
				annotNew.set_ContentElements(celNew);
				annotNew.save(RefreshMode.REFRESH);
			}
			
			annotItStagging = annotationSetStaging.iterator();
			while(annotItStagging.hasNext()) {
				Annotation annot = annotItStagging.next();
				annot.delete();
				annot.save(RefreshMode.REFRESH);
			}

		
		} catch (Exception e) {
			log.error("unhandled CPEUtil Exception");
			log.error(e.toString());
			e.printStackTrace();
		} 
	}
	
	private  InputStream updateAnnotGUID(InputStream is, String targetId) {
		try {
			builder.reset();
			org.w3c.dom.Document doc = builder.parse(is);
			org.w3c.dom.Element elementPropDesc = (org.w3c.dom.Element)doc.getElementsByTagName("PropDesc").item(0);
			elementPropDesc.setAttribute("F_ID", targetId);
			elementPropDesc.setAttribute("F_ANNOTATEDID", targetId);

			ByteArrayOutputStream oStream = new ByteArrayOutputStream();
			Source xmlSource = new DOMSource(doc);
			Result oTarget = new StreamResult(oStream);
			TransformerFactory.newInstance().newTransformer().transform(xmlSource, oTarget);
			InputStream ret = new ByteArrayInputStream(oStream.toByteArray());
			return ret;
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}
	private  InputStream updateAnnotTEXT(InputStream is) {
		try {
			builder.reset();
			org.w3c.dom.Document doc = builder.parse(is);		
			org.w3c.dom.Element elementTEXT = (org.w3c.dom.Element)doc.getElementsByTagName("F_TEXT").item(0);
			
			if(elementTEXT!=null && !"True".equals(elementTEXT.getAttribute("CONVERTED"))) {
				String origText = elementTEXT.getTextContent();
				elementTEXT.setTextContent(big5ToUTF16Mapper.convertFromBig5ToUTF(origText));
				elementTEXT.setAttribute("CONVERTED", "True");
				
				org.w3c.dom.Element elementTEXT_ORIG = doc.createElement("F_TEXT_ORIG");
				org.w3c.dom.Element elementPropDesc = (org.w3c.dom.Element)doc.getElementsByTagName("PropDesc").item(0);
				elementPropDesc.appendChild(elementTEXT_ORIG);
				elementTEXT_ORIG.setTextContent(origText);
			}
			ByteArrayOutputStream oStream = new ByteArrayOutputStream();
			Source xmlSource = new DOMSource(doc);
			Result oTarget = new StreamResult(oStream);
			TransformerFactory.newInstance().newTransformer().transform(xmlSource, oTarget);
			InputStream result = new ByteArrayInputStream(oStream.toByteArray());
			return result;
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}

}
