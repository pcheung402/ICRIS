package com.icris.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
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

import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.w3c.dom.Node;
import org.w3c.dom.Element;

import com.filenet.api.collection.*;
import com.filenet.api.core.*;
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
	private HashMap<String, FileStorageArea> saMap = new HashMap<String, FileStorageArea>(); // Mapping from class name to current storage area
	private HashMap<String, StoragePolicy> spMap= new HashMap<String, StoragePolicy>(); // Mapping from storgae policy name to storage policy object
	private HashMap<String, String> classtoSpMap = new HashMap<String, String>(); // Mapping from class name to storage policy name
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
			big5ToUTF16Mapper = new Big5ToUTFMapper("Big5-HKSCS", "UTF-16BE", log);
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
        	
        	
			this.sps = this.os.get_StoragePolicies();
        	Iterator<StoragePolicy> itsp=sps.iterator();
        	while (itsp.hasNext()) {
        		
        		StoragePolicy temp = itsp.next();
//        		System.out.println(temp.get_Name());
				spMap.put(temp.get_Name(), temp);
        	}
        	
			File fXmlFile = new File( "." + File.separator + "config" + File.separator + "classesDefinitions.xml");
			DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
			DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
			Document doc = dBuilder.parse(fXmlFile);			
			doc.getDocumentElement().normalize();
			NodeList classNodeList = doc.getElementsByTagName("docClass");						
			for (int j = 0; j < classNodeList.getLength(); j++) {
				Node nNode = classNodeList.item(j);
				if (nNode.getNodeType() == Node.ELEMENT_NODE) {					
					Element eElement = (Element) nNode;
					
					NodeList snNodeList = eElement.getElementsByTagName("symName");
					String className = snNodeList.item(0).getTextContent();
					NodeList spNodeList = eElement.getElementsByTagName("StoragePolicy");
					if (spNodeList.getLength()==0) continue;
					String spName = spNodeList.item(0).getTextContent();
					if ("".equals(spName)) continue;

					classtoSpMap.put(className, spName);

		    		/*
		    		 *  Sort all OPEN storage area by CmStandbyActivationPriority
		    		 */
		    		SortedSet<FileStorageArea> saSortedSet = new TreeSet<FileStorageArea>(new Comparator<FileStorageArea>(){
		    			public int compare(FileStorageArea one, FileStorageArea another) {
		    				return one.get_CmStandbyActivationPriority() - another.get_CmStandbyActivationPriority() ;
		    			}
		    		});					
					
		    		if (spName!=null) {
						StorageAreaSet sas = spMap.get(spName).get_StorageAreas();
						Iterator<FileStorageArea> itsa = sas.iterator();
						while (itsa.hasNext()) {
							FileStorageArea temp = itsa.next();
							temp.fetchProperties(new String[] {"ResourceStatus"});
							if(temp.get_ResourceStatus().equals(ResourceStatus.OPEN) ) {
								saSortedSet.add(temp);
							}
						}				
		    		}
					if (!saSortedSet.isEmpty()) {

						this.saMap.put(className, saSortedSet.first());
					}
				}
			}
					isConnected = true;	
					return;
				
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
		}catch (Exception e) {
			System.out.println("unhandled CPEUtil Exception..CPEUtil Constructor");
			e.printStackTrace();
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
	
	public StoragePolicy getStoragePolicy(String docClassName) {
    	return this.spMap.get(classtoSpMap.get(docClassName));
	}
	
	public StorageArea getStorageArea(String docClassName) {
    	return this.saMap.get(docClassName);
	}
	
	public Subject getSubject() {
		return this.sub;
	}
	
	public HashMap<String,Double>  getStorageAreaSize() {
		HashMap<String, Double> saSizeMap = new HashMap<String, Double>();
		
		this.saMap.values().forEach((value)->{
			value.refresh(new String[] {"ContentElementKBytes", "DisplayName"});
			saSizeMap.put(value.get_DisplayName(), value.get_ContentElementKBytes());
			log.info(String.format("%s : %.3f(MB)",value.get_DisplayName(), value.get_ContentElementKBytes()/1024));
		});
		return saSizeMap;
	}
	
	public Integer getCFSISRetries() {
		return this.CFS_IS_Retries;
	}
	

	public Boolean moveContent(com.filenet.api.core.Document doc) throws EngineRuntimeException, ICRISException {
		StorageArea sa = this.saMap.get(doc.getClassName());
		try {
			sa.refresh();
			sa.fetchProperties(new String[] {"Id","DisplayName"});
			doc.moveContent(sa);
			doc.save(RefreshMode.NO_REFRESH);
			return Boolean.TRUE;
		} catch (EngineRuntimeException e) {
			if (e.getExceptionCode().equals(ExceptionCode.CONTENT_FCA_SAVE_FAILED)) {
				log.info("CONTENT_FCA_SAVE_FAILED" + "," + sa.get_DisplayName());
				getNextStorageArea(sa, doc);
				doc.refresh(new String[] {"StorageArea", "NAME"});
				return Boolean.FALSE; /* content move fail, retry, switch to next storage area and retry */
			}
			
			if (e.getExceptionCode().equals(ExceptionCode.CONTENT_SA_STORAGE_AREA_NOT_OPEN)) {
				log.info("CONTENT_SA_STORAGE_AREA_NOT_OPEN"+ "," + sa.get_DisplayName());
//				sa.set_ResourceStatus(ResourceStatus.OPEN);
//				sa.save(RefreshMode.REFRESH);
				getNextStorageArea(sa, doc);
				doc.refresh(new String[] {"StorageArea", "NAME"});
				return Boolean.FALSE; /* content move fail, retry, switch to next storage area and retry */
			}
			
			else if (e.getExceptionCode().equals(ExceptionCode.API_FETCH_MERGE_PROPERTY_ERROR)){
				log.info(String.format("API_FETCH_MERGE_PROPERTY_ERROR , %s",sa.get_DisplayName()));
				e.printStackTrace();
				return Boolean.FALSE;
			
			}
			
//			 if (e.getExceptionCode().equals(ExceptionCode.API_FETCH_MERGE_PROPERTY_ERROR)){ 
//				log.error(String.format("API_FETCH_MERGE_PROPERTY_ERROR , %s",sa.get_DisplayName()));
//				e.printStackTrace();					
//				}
			throw e; /* unhandled exception */
		}
	}
	
	private void getNextStorageArea(StorageArea sa, com.filenet.api.core.Document doc) throws ICRISException{
		try {
			semFileStore.acquire();
			sa.refresh();
			sa.set_ResourceStatus(ResourceStatus.FULL); /* change the current storage area to FULL */
			sa.save(RefreshMode.REFRESH);
//			log.info(String.format("%s is FULL", sa.get_DisplayName()).toString());
			Integer saPri = sa.get_CmStandbyActivationPriority();
			StoragePolicy sp = this.getStoragePolicy(doc.getClassName());
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
				temp.refresh();
				temp.fetchProperties(new String[] {"ResourceStatus"});
				if(temp.get_ResourceStatus().equals(ResourceStatus.STANDBY)||temp.get_ResourceStatus().equals(ResourceStatus.OPEN) ) {
					saSortedSet.add(temp);
				}
			}
			
			/*
			 *  select the first STANDBY or OPEN storage area, if any
			 */
			if (!saSortedSet.isEmpty()) {
				sa = saSortedSet.first();
				sa.setUpdateSequenceNumber(null);
				sa.set_ResourceStatus(ResourceStatus.OPEN);
				sa.save(RefreshMode.REFRESH);
				sa.refresh();
				this.saMap.put(doc.getClassName(),(FileStorageArea)sa);
//				doc.refresh(new String[] {"F_DOCNUMBER","F_DOCCLASSNUMBER","Id","F_PAGES"});
				log.info(String.format("Storage area for %s swithed to %s", doc.getClassName(), sa.get_DisplayName()));				
			} else {
				sa = null;
				throw new ICRISException(ICRISException.ExceptionCodeValue.CPE_SA_UNAVAILABLE, "No more standby Storage Area available"); 
			}	
		} catch (InterruptedException e) {
			throw new ICRISException(ICRISException.ExceptionCodeValue.BM_FILESSTORE_SEM_ARBIRATION_ERROR, "FileStore Semaphore Arbitration Error");
		} finally {
			semFileStore.release();
		}		
		return;
	}

	public Boolean  updateAnnot(com.filenet.api.core.Document doc, FileOutputStream ofs, Boolean previewOnly) {
		Boolean result = false;
		ReplicationGroup rg = doc.get_ReplicationGroup();
		try {			
			if(rg!=null) {
				log.info(String.format("clear replication group of document - %s", doc.get_Name()));
				doc.set_ReplicationGroup(null);
				doc.save(RefreshMode.REFRESH);
//				System.out.println("clear application group");
			}
			AnnotationSet annotSet = doc.get_Annotations();
			

			/*
			 * Loop through all items in annotSet
			 */
			Iterator<Annotation> annotIt = annotSet.iterator();
			while(annotIt.hasNext()) {	/* Begin - iterate all staging annotations */
				Annotation annotOrig = annotIt.next();
//				System.out.println(doc.get_Id() + "," + annotOrig.get_Id());

				
				/*
				 * Create an empty annotation, annotNew
				 */
				Annotation annotNew = Factory.Annotation.createInstance(os, "Annotation");
				
				
				/*
				 * copy the meta data from the annotOrig to annotNew
				 */
				annotNew.set_AnnotatedContentElement(annotOrig.get_AnnotatedContentElement());
				annotNew.setUpdateSequenceNumber(annotOrig.getUpdateSequenceNumber());
				annotNew.set_AnnotatedObject(doc);
				annotNew.set_Permissions(annotOrig.get_Permissions());
				annotNew.save(RefreshMode.REFRESH);
				String annotId = annotNew.get_Id().toString();

				/*
				 * 		isAnnotTextChanged : a flag to check whether any content element in this annotation has been converted.
				 */
				Boolean isAnnotTextChangedFlag = Boolean.FALSE;
				
				/*
				 *  create an empty content element list for annotNew
				 */
				ContentElementList celNew = Factory.ContentElement.createList();
				
				ContentElementList cel = annotOrig.get_ContentElements();
				Iterator<ContentTransfer> ctIt = cel.iterator();
				
				/*
				 * Loop through all content element of annotOrig
				 */
				while (ctIt.hasNext()) {	/* Begin - iterate all content elements in an annotation  */
					
					ContentTransfer ctNew = Factory.ContentTransfer.createInstance();
					ContentTransfer ctOrig = ctIt.next();
//					System.out.println("\t"+ctOrig.toString());
					
					/*
					 *  copy the content ctOrig to ctNew
					 *  update the AnnotId
					 *  convert the F_TEXT of ctNew
					 *  
					 */
					InputStream is = ctOrig.accessContentStream();
					is = updateAnnotGUID(is, annotId, doc, ofs, annotNew, annotOrig, previewOnly);
					HashMap<String, Object> updateResult = updateAnnotTEXT(is, doc, ofs, annotNew, annotOrig, previewOnly);
					is = (InputStream)updateResult.get("ResultStream");
					isAnnotTextChangedFlag = (Boolean)updateResult.get("ChangeFlag");
					/*
					 *  Check whether the F_TEXT has been change (i.e. ASCII or Not)
					 */
					if((Boolean)updateResult.get("ChangeFlag")) {
						/*
						 * F_TEXT and F_TEXT_ORIG does not match, it means there are non-ASCII in F_TEX_ORIG
						 * 
						 * set isAnnotTextChanged to true to indicate there are at least one content element in the current annotation contains non-ASCII
						 * 
						 * add the new content element to the new content element list
						 */
						isAnnotTextChangedFlag = Boolean.TRUE;
						ctNew.setCaptureSource(is);
						ctNew.set_ContentType(ctOrig.get_ContentType());
						ctNew.set_RetrievalName(ctOrig.get_RetrievalName());
						annotNew.save(RefreshMode.REFRESH);
						celNew.add(ctNew);
					} else {
						/*
						 *  F_TEXT and F_TEXT_ORIG  match, it meansF_TEXT_ORIG are ASCII, so it's no need to use the ctNew
						 *  
						 *  add ctOrig to the new content element list
						 */
						celNew.add(ctOrig);
						
					}
					
				}	/* End - iterate all content elements in an annotation  */

			
				if (isAnnotTextChangedFlag) {
					/*
					 *  if any content element of this annotation has been changed :
					 *
					 *	set the new annotation to annotate  doc
					 *  delete annotOrig  
					 */
					result = true;
					if(!previewOnly) {
						annotNew.set_ContentElements(celNew);
						annotNew.save(RefreshMode.REFRESH);
						annotOrig.delete();
						annotOrig.save(RefreshMode.NO_REFRESH);
					} else {
						annotNew.delete();
						annotNew.save(RefreshMode.NO_REFRESH);
					}
				} else {
					/*
					 *  if no content element of this annotation has been changed
					 *  
					 *  delete annotNew
					 *  
					 * 
					 */
					annotNew.delete();
					annotNew.save(RefreshMode.NO_REFRESH);
				}
				
				ofs.flush();
				
				
				
			} /* End - iterate all annotations */
			

		
		} catch (Exception e) {
			log.error("unhandled CPEUtil Exception..update Annot");
			log.error(e.toString());
			e.printStackTrace();
		} 
		
		return result;
	}
	
	
	private  InputStream updateAnnotGUID(InputStream is, String targetId, com.filenet.api.core.Document doc,  FileOutputStream ofs, Annotation annotNew, Annotation annotOrig, Boolean previewOnly) {
		try {
			builder.reset();
			org.w3c.dom.Document annotDoc = builder.parse(is);
			org.w3c.dom.Element elementPropDesc = (org.w3c.dom.Element)annotDoc.getElementsByTagName("PropDesc").item(0);
			elementPropDesc.setAttribute("F_ID", targetId);
			elementPropDesc.setAttribute("F_ANNOTATEDID", targetId);

			ByteArrayOutputStream oStream = new ByteArrayOutputStream();
			Source xmlSource = new DOMSource(annotDoc);
			Result oTarget = new StreamResult(oStream);
			TransformerFactory.newInstance().newTransformer().transform(xmlSource, oTarget);
			InputStream ret = new ByteArrayInputStream(oStream.toByteArray());
			return ret;
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}
	private  HashMap<String, Object> updateAnnotTEXT(InputStream is, com.filenet.api.core.Document doc,  FileOutputStream ofs, Annotation annotNew, Annotation annotOrig, Boolean previewOnly) {
		HashMap<String, Object> result = new HashMap<String, Object>() ;
		com.filenet.api.property.Properties docProperties = doc.getProperties();
		try {
			builder.reset();
			org.w3c.dom.Document annotDoc = builder.parse(is);
			NodeList textNodeList = annotDoc.getElementsByTagName("F_TEXT");
			NodeList origTextNodeList = annotDoc.getElementsByTagName("F_TEXT_ORIG");
			
			if(textNodeList.getLength()==0) { /* F_TEXT does not exist, no changes is required */
//				System.out.println(" F_TEXT does not exist, no changes is required");
				is.reset();
				result.put("ChangeFlag", Boolean.FALSE);
				result.put("ResultStream", is);
				ofs.write(String.format("%s,%s,%s,%10.0f,%d, Annotation Unchanged(F_TEXT does not exist)\n",doc.get_Id().toString(), annotOrig.get_Id().toString(),docProperties.getStringValue("DOC_BARCODE"), docProperties.getFloat64Value("F_DOCNUMBER"),annotOrig.get_AnnotatedContentElement()).getBytes());
			} else {
				org.w3c.dom.Element elementTEXT = (org.w3c.dom.Element)textNodeList.item(0);
				String origText = elementTEXT.getTextContent();
				if (origTextNodeList.getLength()>0) { /* F_TEXT_ORIG exist, it means this annotation has been processed, no changes is required */

				is.reset();
				result.put("ChangeFlag", Boolean.FALSE);
				result.put("ResultStream", is);
				ofs.write(String.format("%s,%s,%s,%10.0f,%d, Annotation Unchanged (F_TEXT_ORIG exist)\n",doc.get_Id().toString(), annotOrig.get_Id().toString(),docProperties.getStringValue("DOC_BARCODE"), docProperties.getFloat64Value("F_DOCNUMBER"),annotOrig.get_AnnotatedContentElement()).getBytes());
				} else if("".equalsIgnoreCase(origText)) { /* F_TEXT is empty, no changes is required */
					is.reset();
					result.put("ChangeFlag", Boolean.FALSE);
					result.put("ResultStream", is);
//					System.out.println(doc.get_Id().toString()+","+annotOrig.get_Id().toString()+","+docProperties.getStringValue("DOC_BARCODE")+","+ String.valueOf(docProperties.getFloat64Value("F_DOCNUMBER"))+","+ String.valueOf(annotOrig.get_AnnotatedContentElement()));
//					if(annotOrig.get_AnnotatedContentElement()!=null) {
//						ofs.write(String.format("%s,%s,%s,%10.f,%d, Annotation Unchanged(F_TEXT is empty)\n",doc.get_Id().toString(), annotOrig.get_Id().toString(),docProperties.getStringValue("DOC_BARCODE"), docProperties.getFloat64Value("F_DOCNUMBER"),annotOrig.get_AnnotatedContentElement()).getBytes());
//					} else {
//						ofs.write(String.format("%s,%s,%s,%10.f,null, Annotation Unchanged(F_TEXT is empty)\n",doc.get_Id().toString(), annotOrig.get_Id().toString(),docProperties.getStringValue("DOC_BARCODE"), docProperties.getFloat64Value("F_DOCNUMBER")).getBytes());
//					}
					ofs.write(String.format("%s,%s,%s,%10.0f,%d, Annotation Unchanged(F_TEXT is empty)\n",doc.get_Id().toString(), annotOrig.get_Id().toString(),docProperties.getStringValue("DOC_BARCODE"), docProperties.getFloat64Value("F_DOCNUMBER"), annotOrig.get_AnnotatedContentElement()).getBytes());
					
				} else {
					String newText = big5ToUTF16Mapper.convertFromBig5ToUTF(origText, doc, annotOrig, ofs);
					elementTEXT.setTextContent(newText);
					if(newText.equalsIgnoreCase(origText)) { /* F_TEXT identical to F_TEXT_ORIG */
//						System.out.println("F_TEXT identical to F_TEXT_ORIG");
						is.reset();
						result.put("ChangeFlag", Boolean.FALSE);
						result.put("ResultStream", is);
						ofs.write(String.format("%s,%s,%s,%10.0f,%d, Annotation Unchanged(F_TEXT identical to F_TEXT_ORIG)\n",doc.get_Id().toString(), annotOrig.get_Id().toString(),docProperties.getStringValue("DOC_BARCODE"), docProperties.getFloat64Value("F_DOCNUMBER"), annotOrig.get_AnnotatedContentElement()).getBytes());
					} else {
						org.w3c.dom.Element elementTEXT_ORIG = annotDoc.createElement("F_TEXT_ORIG");
						org.w3c.dom.Element elementPropDesc = (org.w3c.dom.Element)annotDoc.getElementsByTagName("PropDesc").item(0);
						elementPropDesc.appendChild(elementTEXT_ORIG);
						elementTEXT_ORIG.setTextContent(origText);
						ByteArrayOutputStream oStream = new ByteArrayOutputStream();
						Source xmlSource = new DOMSource(annotDoc);
						Result oTarget = new StreamResult(oStream);
						TransformerFactory.newInstance().newTransformer().transform(xmlSource, oTarget);
						result.put("ChangeFlag", Boolean.TRUE);
						result.put("ResultStream", new ByteArrayInputStream(oStream.toByteArray()));
						if (previewOnly) {
							ofs.write(String.format("%s,%s,%s,%10.0f,%d, Annotation Changed(preview only)\n",doc.get_Id().toString(), annotNew.get_Id().toString(),docProperties.getStringValue("DOC_BARCODE"), docProperties.getFloat64Value("F_DOCNUMBER"), annotNew.get_AnnotatedContentElement()).getBytes());
						} else {
							ofs.write(String.format("%s,%s,%s,%10.0f,%d, Annotation Changed\n",doc.get_Id().toString(), annotNew.get_Id().toString(),docProperties.getStringValue("DOC_BARCODE"), docProperties.getFloat64Value("F_DOCNUMBER"), annotNew.get_AnnotatedContentElement()).getBytes());						
						}
					}
				}
			}
			
		} catch (ICRISException e) {
			log.error(String.format("Annotation Unchanged(%s) : %s,%s,%s,%10.0f,%d",e.getMessage(), doc.get_Id().toString(), annotOrig.get_Id().toString(),docProperties.getStringValue("DOC_BARCODE"), docProperties.getFloat64Value("F_DOCNUMBER"),annotOrig.get_AnnotatedContentElement()));
			log.error(e.getMessage());
			result.put("ChangeFlag", Boolean.FALSE);
			result.put("ResultStream", is);
		} catch (Exception e) {
			e.printStackTrace();
			result = null;
		}
		
		return result;
	}

}
