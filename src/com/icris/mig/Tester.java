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
import com.icris.util.Big5ToUTFMapper;
import com.icris.util.CPEUtil;
import com.icris.util.ICRISException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.util.Iterator;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
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
public class Tester {
	static private CPEUtil revampedCPEUtil;
	static private ObjectStore objectStore = null;
	static private ICRISLogger log;
	static private String docId="{00018A88-B050-5CD6-8998-82E9728C3BC5}";
//	static private String docId="{E04A707E-0000-C81F-A26A-6F591BB24B1A}";
	public static void main(String[] args) {
		// TODO Auto-generated method stub
//		System.out.println("java.security.auth.login.config : "+ System.getProperty("java.security.auth.login.config"));
		try {
			/*
			 * Connect to CPE using CEUtil
			 * modified by icrisicris8
			 * modified by pcheung402
			 * 
			 */
			String serverConfigFile = "revamped.server.conf";
//			String docId = args[1];
			log = new ICRISLogger("Tester","Tester");
			revampedCPEUtil = new CPEUtil(serverConfigFile, log);
			objectStore = revampedCPEUtil.getObjectStore();
			log.info("Connected to : " + revampedCPEUtil.getDomain().get_Name() + ";" +revampedCPEUtil.getObjectStore().get_Name());
//			System.out.println(revampedCPEUtil.getStorageArea("ICRIS_Tmplt_Doc").get_DisplayName());
//			BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("D:\\bigfilemap.csv")));
//			Big5ToUTFMapper big5ToUTFMapper=new Big5ToUTFMapper("Big5-HKSCS", "UTF-16BE", log);
//			big5ToUTFMapper.printMap(bw);
//			String utfStr = big5ToUTFMapper.convertFromBig5ToUTF("00A100B10020002000A100C2002000A20058002000A100D3002000A10050002000A100D1002000A100D20020002000A300BE002000A300BC002000A300BD002000A300BF002000A100C5002000A300BB002000A30044002000A30045002000A30046002000A30047002000A30048002000A30049002000A3004A002000A3004B002000A3004C002000A3004D002000A3004E002000A3004F002000A30050002000A30051002000A30052002000A30053002000A30054002000A30055002000A30056002000A30057002000A30058002000A30059002000A3005A002000A3005B002000A3005C002000A3005D002000A3005E002000A3005F002000A30060002000A30061002000A30062002000A30063002000A30064002000A30065002000A30066002000A30067002000A30068002000A30069002000A3006A002000A3006B002000A3006C002000A3006D002000A3006E002000A3006F002000A30070002000A30071002000A30072002000A3007300200020002D0020002D00200027002000270020002200200022002000A1004C002000A1004B002000A10045002000A100AC002000A100AB002000A100B0002000A300E1002000A2004A002000A100C1002000A2004B002000A200B9002000A200BA002000A200BB002000A200BC002000A200BD002000A200BE002000A200BF002000A200C0002000A200C1002000A200C2002000A100F6002000A100F4002000A100F7002000A100F5002000A100F8002000A100F9002000A100FB002000A100FA002000A20041002000A100D4002000A100DB002000A100E8002000A100E7002000A100FD002000A100FC002000A100E4002000A100E5002000A100EC002000A100ED002000A100EF002000A100EE002000A100DC002000A100DA002000A100DD002000A100D8002000A100D9002000A100F2002000A100F3002000A100E6002000A100E9000A");
//			System.out.println(utfStr);
//			bw.flush();
			
			
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
//			log.error("unhandled CPEUtil Exception");
//			log.error(e.toString());
			e.printStackTrace();
		}
	}
}
