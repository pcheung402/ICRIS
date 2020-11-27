package com.icris.mig;

import java.io.File;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

public class ReadClassesDefinitionFromXML {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		try {			
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
					System.out.println("Parent Class : " + eElement.getElementsByTagName("parentClass").item(0).getTextContent());
					System.out.println("Symbolic Name : " + eElement.getElementsByTagName("symName").item(0).getTextContent());
					NodeList propNodeList = eElement.getElementsByTagName("property");
					System.out.println("Properties :");
					for (int i = 0; i < propNodeList.getLength(); ++i) {
						Node propNode = propNodeList.item(i);
						Element propElement = (Element) propNode;
						if(propNode.getNodeType()==Node.ELEMENT_NODE)
							System.out.println(propElement.getTextContent());
					}					
				}
			} 
		} catch (Exception e) {
			
		}
	}

}
