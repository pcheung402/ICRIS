package com.icris.mig;
import java.util.ArrayList;
import java.util.HashMap;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.Properties;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;

import javax.security.auth.Subject;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.icris.util.CPEUtil;
import com.icris.util.CSVParser;
import com.icris.util.ICRISException;
import com.icris.util.ICRISLogger;
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
import com.filenet.api.collection.LocalizedStringList;
import com.filenet.api.admin.LocalizedString;
import com.filenet.api.constants.Cardinality;
public class CreatePropertyTemplates {
	static private CPEUtil revampedCPEUtil;
	static private ICRISLogger log;
	static private ObjectStore objectStore = null;
	static Properties prop = new Properties();	
	static ArrayList<HashMap<String,String>> PropertiesList;

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		CSVParser csvParser = new CSVParser();
		
		try {
//			PropertiesList = csvParser.parser(new FileInputStream(new File("config\\properties_list.csv")));
			log = new ICRISLogger("CreateFileStorageArea",null);
			revampedCPEUtil = new CPEUtil("revamped.server.conf", log);
			objectStore = revampedCPEUtil.getObjectStore();
			
			File fXmlFile = new File( "." + File.separator + "config" + File.separator + "propertiesDefinitions.xml");
			DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
			DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
			Document doc = dBuilder.parse(fXmlFile);			
			doc.getDocumentElement().normalize();
			NodeList propertyNodeList = doc.getElementsByTagName("property");						
			for (int j = 0; j < propertyNodeList.getLength(); j++) {
				Node nNode = propertyNodeList.item(j);
				if (nNode.getNodeType() == Node.ELEMENT_NODE) {
					Element eElement = (Element) nNode;
					createPropertyTemplate(eElement.getElementsByTagName("symName").item(0).getTextContent(),
							eElement.getElementsByTagName("description").item(0).getTextContent(),
							eElement.getElementsByTagName("dataType").item(0).getTextContent(),
							eElement.getElementsByTagName("dataLength").item(0).getTextContent());
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
		} catch (IOException e){
			System.out.println(e.getMessage());	
		} catch (Exception e) {
			System.out.println("unhandled CPEUtil Exception");
			System.out.println(e.toString());
		}
		
//		for (int i=0; i < PropertiesList.size(); ++i){
//			createPropertyTemplate(PropertiesList.get(i));
//		}
	}
//	static private void createPropertyTemplate(HashMap<String,String> propertyTemplateDef){
	static private void createPropertyTemplate(String symbolicName, String displayName, String dataType, String dataLength ){
		System.out.println(
				symbolicName + "," +
				displayName + "," +
				dataType + "," +
				dataLength
		);
		
		if ("String".equalsIgnoreCase(dataType)) {
			createPropertyTemplateString(symbolicName, displayName);
		} else if ("DateTime".equalsIgnoreCase(dataType)){
			createPropertyTemplateDate(symbolicName, displayName);
		} else if ("Double".equalsIgnoreCase(dataType)){
			createPropertyTemplateDouble(symbolicName, displayName);
		} else if ("Integer".equalsIgnoreCase(dataType)){
			createPropertyTemplateInteger(symbolicName, displayName);
		}
	}
	static private void createPropertyTemplateString(String symbolicName, String displayName){
		PropertyTemplateString pt = Factory.PropertyTemplateString.createInstance(objectStore);
		pt.set_Cardinality (Cardinality.SINGLE);
		pt.set_SymbolicName(symbolicName);
		pt.set_DisplayNames(Factory.LocalizedString.createList());
		LocalizedString ls = Factory.LocalizedString.createInstance(objectStore);
		ls.set_LocaleName(objectStore.get_LocaleName());
		ls.set_LocalizedText(displayName);
		pt.get_DisplayNames().add(ls);
		pt.save(RefreshMode.REFRESH);
	}
	
	static private void createPropertyTemplateDate(String symbolicName, String displayName){
		PropertyTemplateDateTime pt = Factory.PropertyTemplateDateTime.createInstance(objectStore);
		pt.set_Cardinality (Cardinality.SINGLE);
		pt.set_SymbolicName(symbolicName);
		pt.set_DisplayNames(Factory.LocalizedString.createList());
		LocalizedString ls = Factory.LocalizedString.createInstance(objectStore);
		ls.set_LocaleName(objectStore.get_LocaleName());
		ls.set_LocalizedText(displayName);
		pt.get_DisplayNames().add(ls);
		pt.save(RefreshMode.REFRESH);	
	}	

	static private void createPropertyTemplateDouble(String symbolicName, String displayName){
		PropertyTemplateFloat64 pt = Factory.PropertyTemplateFloat64.createInstance(objectStore);
		pt.set_Cardinality (Cardinality.SINGLE);
		pt.set_SymbolicName(symbolicName);
		pt.set_DisplayNames(Factory.LocalizedString.createList());
		LocalizedString ls = Factory.LocalizedString.createInstance(objectStore);
		ls.set_LocaleName(objectStore.get_LocaleName());
		ls.set_LocalizedText(displayName);
		pt.get_DisplayNames().add(ls);
		pt.save(RefreshMode.REFRESH);	
	}
	
	static private void createPropertyTemplateInteger(String symbolicName, String displayName){
		PropertyTemplateInteger32 pt = Factory.PropertyTemplateInteger32.createInstance(objectStore);
		pt.set_Cardinality (Cardinality.SINGLE);
		pt.set_SymbolicName(symbolicName);
		pt.set_DisplayNames(Factory.LocalizedString.createList());
		LocalizedString ls = Factory.LocalizedString.createInstance(objectStore);
		ls.set_LocaleName(objectStore.get_LocaleName());
		ls.set_LocalizedText(displayName);
		pt.get_DisplayNames().add(ls);
		pt.save(RefreshMode.REFRESH);	
	}	
	
}
