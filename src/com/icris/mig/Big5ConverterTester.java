package com.icris.mig;

import java.io.IOException;

import com.filenet.api.collection.ContentElementList;
import com.filenet.api.constants.RefreshMode;
import com.filenet.api.core.Annotation;
import com.filenet.api.core.ContentTransfer;
import com.filenet.api.core.Document;
import com.filenet.api.core.Factory;
import com.filenet.api.core.ObjectStore;
import com.filenet.api.util.*;
import com.filenet.api.collection.*;
import com.filenet.api.replication.*;
import com.icris.util.ICRISLogger;
import com.icris.util.CPEUtil;
import com.icris.util.ICRISException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Iterator;
import java.io.InputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;
import org.w3c.dom.Element;

import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.TransformerFactoryConfigurationError;
import javax.xml.transform.Source;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.Result;

import java.util.*;
public class Big5ConverterTester {
	static private CPEUtil revampedCPEUtil;
	static private ObjectStore objectStore = null;
	static private ICRISLogger log;
//	static private String docId="{00018A88-B050-5CD6-8998-82E9728C3BC5}";
//	static private String docId="{E04A707E-0000-C81F-A26A-6F591BB24B1A}";
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		System.out.println("java.security.auth.login.config : "+ System.getProperty("java.security.auth.login.config"));
		try {
			/*
			 * Connect to CPE using CEUtil
			 * modified by icrisicris8
			 * modified by pcheung402
			 * 
			 */
			String serverConfigFile = args[0];
			String docId = args[1];
			log = new ICRISLogger("Tester","Tester");
			revampedCPEUtil = new CPEUtil(serverConfigFile, log);
			objectStore = revampedCPEUtil.getObjectStore();
			log.info("Connected to : " + revampedCPEUtil.getDomain().get_Name() + ";" +revampedCPEUtil.getObjectStore().get_Name());
			Document doc = Factory.Document.fetchInstance(objectStore, new Id(docId), null);
			doc.fetchProperties(new String[] {"Id", "DOC_BARCODE", "F_DOCNUMBER"});
//			System.out.println(docId);
			ReplicationGroup rg=doc.get_ReplicationGroup();
			String annotConvertOutputDataFilePath = "." + File.separator + "data" + File.separator + "annotConvertOutput" + File.separator + "annotConvertOutputTest.dat";
			Files.deleteIfExists(Paths.get(annotConvertOutputDataFilePath));
			FileOutputStream annotConvertOutputDataFile = new FileOutputStream(annotConvertOutputDataFilePath);
			revampedCPEUtil.updateAnnot(doc, annotConvertOutputDataFile, Boolean.FALSE);
			log.info("finished");

			
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
			e.printStackTrace();
		}
	}
}
