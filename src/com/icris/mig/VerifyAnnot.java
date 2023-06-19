package com.icris.mig;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Iterator;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import java.util.HashMap;
import java.util.ArrayList;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Calendar;

import com.filenet.api.core.ObjectStore;
import com.filenet.api.query.SearchSQL;
import com.filenet.api.query.SearchScope;
import com.filenet.api.util.Id;
import com.filenet.api.core.Document;
import com.filenet.api.core.Factory;
import com.filenet.api.core.ContentElement;
import com.filenet.api.core.ContentTransfer;
import com.filenet.api.core.Annotation;
import com.filenet.api.property.*;
import com.filenet.api.collection.*;
import com.filenet.api.constants.RefreshMode;
import com.filenet.api.query.*;
import com.icris.util.ICRISLogger;
import com.icris.util.CPEUtil;
import com.icris.util.ICRISException;
import com.icris.util.CSVParser;

public class VerifyAnnot {
//	static String docidSetId;
	static ObjectStore objectStore;
	static ICRISLogger log;
	static BufferedReader brDocidList = null;
	static FileOutputStream verifiedSuccesOutputDataFile;
	static FileOutputStream verifiedFailedOutputDataFile;
	static SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yyyy");
	static private CPEUtil revampedCPEUtil;
	static private CSVParser csvParser = new CSVParser();
	static private HashMap<Integer, String> classNumToSymNameMap = new HashMap<Integer, String>();
//	static FileOutputStream invalidAnnotSizeReport_A, invalidAnnotSizeReport_O;
//	static Double widthThreshold = 0.0;
//	static Double heigthThreshold = 0.0;
//	static private String[] classSymNameArray = {"","","","","",""};
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		initialize(args);
		try {

			String line;
			log.info(String.format("Start verify annotation of documents ..."));
			while ((line=brDocidList.readLine())!=null) {
				String[] parsedTokens = csvParser.lineParser(line);
				Integer noAnnot = Integer.valueOf( parsedTokens[0]);
				String docGUID = parsedTokens[1];
//				System.out.println(parsedTokens[0] +","+docGUID);
				Document doc = Factory.Document.fetchInstance(revampedCPEUtil.getObjectStore(), new Id(docGUID), null);
				doc.fetchProperties(new String[] {"Id", "Annotations"});
				AnnotationSet annotSet = doc.get_Annotations();
				Iterator itAnnot = annotSet.iterator();
				Integer i = 0;
				while (itAnnot.hasNext()) {
					Annotation annot = (Annotation)itAnnot.next();
//					annot.delete();
//					annot.save(RefreshMode.NO_REFRESH);
					++i;
				}
				
				if(i!=noAnnot) {
					log.info(String.format("", noAnnot, i, doc.get_Id().toString()));
				}

			}
			log.info(String.format("End verify annotation of documents ..."));
		} catch (IOException e) {
			log.error(e.getMessage());
		}	catch (Exception e) {
			e.printStackTrace();
		}
	}
	private static void initialize(String[] args) {
		classNumToSymNameMap.put(1, "ICRIS_Pend_Doc");
		classNumToSymNameMap.put(2, "ICRIS_Reg_Doc");
		classNumToSymNameMap.put(3, "ICRIS_Tmplt_Doc");
		classNumToSymNameMap.put(4, "ICRIS_Migrate_Doc");
		classNumToSymNameMap.put(5, "ICRIS_CR_Doc");
		classNumToSymNameMap.put(6, "ICRIS_BR_Doc");
		try {
			log = new ICRISLogger("verifyAnnotations",null);
			revampedCPEUtil = new CPEUtil("revamped.server.conf", log);
			objectStore = revampedCPEUtil.getObjectStore();
			brDocidList = new BufferedReader(new FileReader(new File("./data/ver_anot.lst")));
		} catch (ICRISException e) {
			if (e.exceptionCode.equals(ICRISException.ExceptionCodeValue.CPE_CONFIG_FILE_NOT_FOUND)) {
				log.error(e.getMessage());				
			} else if (e.exceptionCode.equals(ICRISException.ExceptionCodeValue.CPE_CONFIG_FILE_CANNOT_BE_OPENED)) {
				log.error(e.getMessage());				
			}
		}  catch (Exception e) {
			log.error("unhandled Exception");
			log.error(e.toString());
		}
	}

}


