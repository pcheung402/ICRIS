package com.icris.mig;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.Properties;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import javax.security.auth.Subject;


import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.DocumentBuilder;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.w3c.dom.Node;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;

import com.icris.util.CPEUtil;
import com.icris.util.CSVParser;
import com.icris.util.ICRISException;
import com.icris.util.ICRISLogger;
import com.filenet.api.constants.RefreshMode;
import com.filenet.api.core.Connection;
import com.filenet.api.core.Domain;
import com.filenet.api.core.Factory;
import com.filenet.api.core.ObjectStore;
import com.filenet.api.core.Subscribable;
import com.filenet.api.util.*;
import com.filenet.api.admin.*;
import com.filenet.api.collection.*;
import com.filenet.api.constants.Cardinality;
import com.filenet.api.constants.PropertyNames;
public class CreateFileStorageArea {
	
	static private CPEUtil revampedCPEUtil;
	static private ICRISLogger log;
	static private ObjectStore objectStore = null;

	
	public static void main(String[] args) {

		// TODO Auto-generated method stub
		try {
			log = new ICRISLogger("CreateFileStorageArea",null);
			revampedCPEUtil = new CPEUtil("revamped.server.conf", log);
			objectStore = revampedCPEUtil.getObjectStore();
			File fXmlFile = new File( "." + File.separator + "config" + File.separator + "storageAreaDefinitions.xml");
			DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
			DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
			Document doc = dBuilder.parse(fXmlFile);			
			doc.getDocumentElement().normalize();
			NodeList storagePolicyNodeList = doc.getElementsByTagName("StoragePolicy");
			Node storagePolicyNode = storagePolicyNodeList.item(0);
//			StoragePolicy sp = Factory.StoragePolicy.fetchInstance(objectStore, new Id(storagePolicyNode.getAttributes().getNamedItem("GUID").getNodeValue()), null);
//			sp.refresh(new String[]{"StorageAreas"});
//			StorageAreaSet saSet = sp.get_StorageAreas(); 
//			Iterator it = saSet.iterator();
//			while(it.hasNext()) {
//				it.remove();
//			}
			
//			if (saSet.isEmpty()) System.out.println("Storage policy is empty");
			NodeList storageAreaNodeList = storagePolicyNode.getChildNodes();

			for (int j = 0; j < storageAreaNodeList.getLength(); j++) {
				Node storageAreaNode =storageAreaNodeList.item(j);
//				Element storageAreaElement =(Element) storageAreaNode;
				if(storageAreaNode.getNodeName().equals("StorageArea")) {
					NamedNodeMap nnm = storageAreaNode.getAttributes();
					String rootDirectoryPath = nnm.getNamedItem("rootDirectoryPath").getNodeValue();
					String fsaDisplayName = storageAreaNode.getTextContent();
					FileStorageArea fsa = createFileStorageArea(fsaDisplayName,rootDirectoryPath);
				}
			}
								
		} catch (ICRISException e) {
			if (e.exceptionCode.equals(ICRISException.ExceptionCodeValue.CPE_USNAME_PASSWORD_INVALID)) {
				System.out.println(e.getMessage());				
			} else if (e.exceptionCode.equals(ICRISException.ExceptionCodeValue.CPE_URI_INVALID)) {
				System.out.println(e.getMessage());				
			} else if (e.exceptionCode.equals(ICRISException.ExceptionCodeValue.CPE_INVALID_OS_NAME)) {
				System.out.println(e.getMessage());				
			} else if (e.exceptionCode.equals(com.icris.util.ICRISException.ExceptionCodeValue.BM_LOAD_BATCH_SET_CONFIG_ERROR)) {
				System.out.println(e.getMessage());
			}
		} catch (Exception e) {
			System.out.println("unhandled CPEUtil Exception");
			e.printStackTrace();
		}
	}
	
	private static FileStorageArea createFileStorageArea(String fsaDisplayName, String rootDirectoryPath) throws Exception{
		System.out.print(String.format("%s, %s\n", fsaDisplayName, rootDirectoryPath));
		FileStorageArea fsa = Factory.FileStorageArea.createInstance(objectStore, "FileStorageArea", null);
		fsa.set_RootDirectoryPath(rootDirectoryPath);
		fsa.set_CmCompressionEnabled(false);
		fsa.set_DuplicateSuppressionEnabled(false);
		fsa.set_DisplayName(fsaDisplayName);
		fsa.save(RefreshMode.NO_REFRESH);
		return fsa;
	}

}
