package com.icris.mig;

import java.io.File;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

public class ReadPropertiesDefinitionFromXML {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		try {			
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
					System.out.println("Symbolic Name : " + eElement.getElementsByTagName("symName").item(0).getTextContent());
					System.out.println("Description : " + eElement.getElementsByTagName("description").item(0).getTextContent());
					System.out.println("Data Type : " + eElement.getElementsByTagName("dataType").item(0).getTextContent());
					System.out.println("Data Length : " + eElement.getElementsByTagName("dataLength").item(0).getTextContent());				
				}
			} 
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
