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

import com.icris.util.CPEUtil;
import com.icris.util.CSVParser;
import com.icris.util.ICRISException;
import com.filenet.api.constants.RefreshMode;
import com.filenet.api.core.Connection;
import com.filenet.api.core.Domain;
import com.filenet.api.core.Factory;
import com.filenet.api.core.ObjectStore;
import com.filenet.api.util.UserContext;
import com.filenet.api.admin.PropertyTemplateString;
import com.filenet.api.admin.PropertyTemplateDateTime;
import com.filenet.api.admin.PropertyTemplateFloat64;
import com.filenet.api.admin.PropertyTemplateInteger32;
import com.filenet.api.admin.PropertyTemplate;
import com.filenet.api.admin.ClassDefinition;
import com.filenet.api.admin.PropertyDefinition;
import com.filenet.api.collection.LocalizedStringList;
import com.filenet.api.collection.PropertyDefinitionList;
import com.filenet.api.collection.PropertyTemplateSet;
import com.filenet.api.admin.LocalizedString;
import com.filenet.api.constants.Cardinality;
import com.filenet.api.constants.PropertyNames;
import com.icris.util.ICRISLogger;

public class CreateDocumentClass {
	
	static private CPEUtil revampedCPEUtil;
	static private ICRISLogger log;
	static private ObjectStore objectStore = null;

	
	public static void main(String[] args) {

		// TODO Auto-generated method stub
		try {
			log = new ICRISLogger("CreateDocumentClass",null);
			revampedCPEUtil = new CPEUtil("revamped.server.conf",log);
			objectStore = revampedCPEUtil.getObjectStore();
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
//					System.out.println("Parent Class : " + eElement.getElementsByTagName("parentClass").item(0).getTextContent());
//					System.out.println("Symbolic Name : " + eElement.getElementsByTagName("symName").item(0).getTextContent());
					NodeList propNodeList = eElement.getElementsByTagName("property");
//					System.out.println("Properties :");
					ArrayList<String> propSymbolicNames = new ArrayList<String>();
					for (int i = 0; i < propNodeList.getLength(); ++i) {
						Node propNode = propNodeList.item(i);
						Element propElement = (Element) propNode;
						if(propNode.getNodeType()==Node.ELEMENT_NODE) {
//							System.out.println(propElement.getTextContent());
							propSymbolicNames.add(propElement.getTextContent());
						}							
					}					
					createClass(eElement.getElementsByTagName("symName").item(0).getTextContent(),eElement.getElementsByTagName("parentClass").item(0).getTextContent(), propSymbolicNames);
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
	
	private static PropertyTemplate getPropertyTemplateBySymbolicName(String symbolicName) {
		String[] properties = {PropertyNames.PROPERTY_TEMPLATES};
		objectStore.fetchProperties(properties);
		PropertyTemplateSet propertyTemplates = objectStore.get_PropertyTemplates();
		Iterator<?> iterator = propertyTemplates.iterator();
		while (iterator.hasNext()) {
		    PropertyTemplate propertyTemplate = (PropertyTemplate) iterator.next();
		    String[] arg = {PropertyNames.SYMBOLIC_NAME, PropertyNames.NAME};
		    propertyTemplate.fetchProperties(arg);
		    if (propertyTemplate.get_SymbolicName().equals(symbolicName)) {
		       //do some stuff
//			    System.out.println(propertyTemplate.get_SymbolicName() + ";" + symbolicName);
		    	return propertyTemplate;
		    }
		}
		return null;
	}
	
	private static void createClass(String newClassDefinitionSymbolicName, String parentClassDefinitionSymbolicName, ArrayList<String> propSymbolicNames) throws ICRISException, Exception {
		ClassDefinition cdParent = Factory.ClassDefinition.fetchInstance(objectStore, parentClassDefinitionSymbolicName, null);
		ClassDefinition cd = cdParent.createSubclass();
		LocalizedString lsDisplayName = Factory.LocalizedString.createInstance(objectStore);
		lsDisplayName.set_LocaleName(objectStore.get_LocaleName());
		lsDisplayName.set_LocalizedText(newClassDefinitionSymbolicName);
		cd.set_DisplayNames(Factory.LocalizedString.createList());
		cd.get_DisplayNames().add(lsDisplayName);
		
		LocalizedString lsDescriptiveText = Factory.LocalizedString.createInstance(objectStore);
		lsDescriptiveText.set_LocaleName(objectStore.get_LocaleName());
		lsDescriptiveText.set_LocalizedText("ICRIS Pending Document Class");
		cd.set_DescriptiveTexts(Factory.LocalizedString.createList());			
		cd.get_DescriptiveTexts().add(lsDescriptiveText);
		cd.set_SymbolicName(newClassDefinitionSymbolicName);
		cd.save(RefreshMode.REFRESH);			
		PropertyDefinitionList pdl = cd.get_PropertyDefinitions();	
		for (String propSymbolicName : propSymbolicNames) {
			PropertyTemplate pt = getPropertyTemplateBySymbolicName(propSymbolicName);
			PropertyDefinition pd = (PropertyDefinition)pt.createClassProperty();
			pdl.add(pd);
		}
		cd.save(RefreshMode.REFRESH);		
	}

}
