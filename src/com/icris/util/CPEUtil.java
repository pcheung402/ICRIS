package com.icris.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.FileNotFoundException;
import java.util.Properties;
import java.util.Iterator;
import java.util.Date;
import java.nio.file.Paths;
import javax.security.auth.Subject;
import com.filenet.api.collection.*;
import com.filenet.api.core.Connection;
import com.filenet.api.core.Domain;
import com.filenet.api.core.Factory;
import com.filenet.api.core.ObjectStore;
import com.filenet.api.core.Document;
import com.filenet.api.exception.EngineRuntimeException;
import com.filenet.api.exception.ExceptionCode;
import com.filenet.api.util.UserContext;
import com.filenet.api.admin.*;
import com.filenet.api.constants.*;



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
	
	public CPEUtil(String configFile,ICRISLogger log) throws ICRISException{
		this.log = log;
		CONF_FILE_DIR = "." + File.separator + "config" + File.separator + configFile;
		System.out.println("configure file : " + CONF_FILE_DIR);
		try {
			InputStream input = new FileInputStream(CONF_FILE_DIR);
	        Properties props = new Properties();
	        // load a properties file
	        props.load(input);
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
        	if(os==null) {
        		throw new ICRISException(ICRISException.ExceptionCodeValue.CPE_INVALID_OS_NAME, "Invalid Objec Store Name in File revamp.server.conf");
        	}
        	sps = os.get_StoragePolicies();
        	Iterator<StoragePolicy> itsp=sps.iterator();
        	while (itsp.hasNext()) {
        		StoragePolicy temp = itsp.next();
        		if (temp.get_Name().equals(props.getProperty("CPESP"))) {
        			this.sp = temp;
        			break;
        		}
        	}
        	
        	if (sp==null) {
        		throw new ICRISException(ICRISException.ExceptionCodeValue.CPE_INVALID_SP_NAME, "Invalid Storage Policy Name in File revamp.server.conf");        		
        	}
        	
        	sas = sp.get_StorageAreas();
			Iterator<FileStorageArea> itsa = sas.iterator();
			while (itsa.hasNext()) {
				FileStorageArea temp = itsa.next();
				temp.fetchProperties(new String[] {"ResourceStatus"});
				if(temp.get_ResourceStatus().equals(ResourceStatus.OPEN)) {
					sa = temp;
					break;
				}
			}
        	if (sa==null) {
        		throw new ICRISException(ICRISException.ExceptionCodeValue.CPE_INVALID_SA_NAME, "No Valid Storagfe Area is Found");        		
        	}
	        isConnected = true;					
		} catch (FileNotFoundException e) {
			throw new ICRISException(ICRISException.ExceptionCodeValue.CPE_CONFIG_FILE_NOT_FOUND, "File " + CONF_FILE_DIR + " not found", e);
		} catch (IOException e) {
			throw new ICRISException(ICRISException.ExceptionCodeValue.CPE_CONFIG_FILE_CANNOT_BE_OPENED, "Error in opening/reading server.conf.properties file", e);
		} catch (IllegalArgumentException e) {
			throw new ICRISException(ICRISException.ExceptionCodeValue.CPE_CONFIG_FILE_ILLEGAL_ARGUMENT, "Illegal argument in server.conf.properties", e);
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
		return sa.get_ContentElementKBytes();
//		return 0.0;
	}
	

	public Boolean moveContent(Document doc) throws EngineRuntimeException, ICRISException {
		try {
			doc.moveContent(sa);
			doc.save(RefreshMode.NO_REFRESH);
			return Boolean.TRUE;
		} catch (EngineRuntimeException e) {
			if (e.getExceptionCode().equals(ExceptionCode.CONTENT_FCA_SAVE_FAILED)) {
				log.info("CONTENT_FCA_SAVE_FAILED" + "," + sa.get_DisplayName());
				FileStorageArea newSa = getNextStorageArea();
				sa = newSa;
				sa.set_ResourceStatus(ResourceStatus.OPEN);
				sa.save(RefreshMode.REFRESH);
				sa.refresh();
				doc.refresh(new String[] {"StorageArea", "NAME"});
//				System.out.println("switch to next Storage Area" + "," + sa.get_DisplayName());
				return Boolean.FALSE;
			}
			
			if (e.getExceptionCode().equals(ExceptionCode.CONTENT_SA_STORAGE_AREA_NOT_OPEN)) {
				log.info("CONTENT_SA_STORAGE_AREA_NOT_OPEN"+ "," + sa.get_DisplayName());
				sa.set_ResourceStatus(ResourceStatus.OPEN);
				sa.save(RefreshMode.REFRESH);				
				sa.refresh();
				return Boolean.FALSE;
			}
			throw e;
		}
	}
	
	private FileStorageArea getNextStorageArea() throws ICRISException, ICRISException{
		Integer saPri = sa.get_CmStandbyActivationPriority();
		StoragePolicy sp = getStoragePolicy();
		StorageAreaSet sas = sp.get_StorageAreas();
		Iterator<FileStorageArea> itsa = sas.iterator();
		while (itsa.hasNext()) {
			FileStorageArea temp = itsa.next();
			temp.fetchProperties(new String[] {"ResourceStatus"});
//			System.out.println("this SA standby priority" + ":" + temp.get_CmStandbyActivationPriority());
			if(temp.get_ResourceStatus().equals(ResourceStatus.STANDBY) && temp.get_CmStandbyActivationPriority() > saPri) {
				sa.set_ResourceStatus(ResourceStatus.FULL);
				sa.save(RefreshMode.REFRESH);
				log.info(String.format("%s is FULL", sa.get_DisplayName()).toString());
				temp.set_ResourceStatus(ResourceStatus.OPEN);
				temp.save(RefreshMode.REFRESH);
//				System.out.println("Switch to next  : " + ":" + temp.get_DisplayName()+ "," + temp.get_ResourceStatus().toString());
				log.info(String.format("%s is OPENed", temp.get_DisplayName()).toString());
				return temp;
			}
		}
		
		throw new ICRISException(ICRISException.ExceptionCodeValue.CPE_SA_UNAVAILABLE, "No more stnadby Storage Area available"); 
	}	
}
