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
import java.util.HashMap;
import java.util.ArrayList;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Calendar;

import com.filenet.api.core.ObjectStore;
import com.filenet.api.core.ContentTransfer;
import com.filenet.api.query.SearchSQL;
import com.filenet.api.query.SearchScope;
import com.filenet.api.core.Document;
import com.filenet.api.core.Factory;
import com.filenet.api.core.ContentElement;
import com.filenet.api.core.Annotation;
import com.filenet.api.property.*;
import com.filenet.api.collection.*;
import com.filenet.api.constants.RefreshMode;
import com.filenet.api.query.*;
import com.filenet.api.util.*;
import com.filenet.api.collection.*;
import com.icris.util.ICRISLogger;
import com.icris.util.CPEUtil;
import com.icris.util.ICRISException;
import com.icris.util.CSVParser;

public class CountDocStorage {
	static private CPEUtil revampedCPEUtil;
	static private ObjectStore objectStore = null;
	static private CSVParser csvParser = new CSVParser();
	static private Double totalDocStorage = 0.0;
	static ICRISLogger log;
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		initialize(args);
		Integer i = 0;
		try {
			BufferedReader br = new BufferedReader(new FileReader(new File(args[0])));
			String line;
			while ((line=br.readLine())!=null) {
				String[] tokenized= csvParser.lineParser(line);
				String docId = tokenized[0];
//				System.out.println(docId);
				totalDocStorage += countDocStorage(new Id(docId));
//				System.out.print(String.format("Total Storage : \n%10.0f\n", totalDocStorage/(1024*1024)));
				++i;
				if (i%1000 == 0) {
					System.out.print(".");
				}
				if(i%50000 == 0) {
					System.out.println();
				}
			}
			
			System.out.print(String.format("\nTotal Storage : %10.2f (MB)\n", totalDocStorage/(1024*1024)));

		} catch (IOException e) {
			e.printStackTrace();
		}	catch (Exception e) {
			e.printStackTrace();
		}
	}
	private static void initialize(String[] args) {
		try {
			log = new ICRISLogger("CountDocStorage",null);
			revampedCPEUtil = new CPEUtil("revamped.server.conf", log);
			objectStore = revampedCPEUtil.getObjectStore();
			System.out.println("Connected to : " + revampedCPEUtil.getDomain().get_Name() + ";" +revampedCPEUtil.getObjectStore().get_Name());
		} catch (ICRISException e) {
			if (e.exceptionCode.equals(ICRISException.ExceptionCodeValue.CPE_CONFIG_FILE_NOT_FOUND)) {
				e.printStackTrace();
			} else if (e.exceptionCode.equals(ICRISException.ExceptionCodeValue.CPE_CONFIG_FILE_CANNOT_BE_OPENED)) {
				e.printStackTrace();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private static Double countDocStorage(Id docId) {
		Double docStorage = 0.0;		
		Document doc = Factory.Document.fetchInstance(objectStore, docId, null);
		ContentElementList cel = doc.get_ContentElements();
		Iterator celIt = cel.iterator();
		while (celIt.hasNext()) {
			ContentTransfer ct= (ContentTransfer)celIt.next();
			docStorage +=ct.get_ContentSize();
		}
//		System.out.println(docStorage);
		return docStorage;
	}
}


