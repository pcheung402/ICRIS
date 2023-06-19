package com.icris.mig;

import org.apache.log4j.*;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.*;
import com.icris.util.*;
import com.filenet.api.exception.EngineRuntimeException;
import com.filenet.api.exception.ExceptionCode;
import com.filenet.api.util.UserContext;


public class ConvertAnnotFromBig5ToUnicode {
	static String batchSetId;
	static ICRISLogger log;
	static BulkMoverManager bmManager;
	static CPEUtil revampedCPEUtil;
	static FileOutputStream annotConvertOutputDataFile;
	public static void main(String[] args) {
		// TODO Auto-generated method stub
//		HashMap<String, Double>  intialSASize, finalSASize;
		initialize(args);
		log.info("Connected to : " + revampedCPEUtil.getDomain().get_Name() + ";" +revampedCPEUtil.getObjectStore().get_Name());
		ExecutorService es = Executors.newCachedThreadPool();
		File folder = new File("data/annotConvertBatches/batchSets/" + batchSetId);
//		System.out.println("data folder : " + "data/annotConvertBatches/batchSets/" + batchSetId);
		File[] listOfFiles = folder.listFiles();
//		intialSASize = revampedCPEUtil.getStorageAreaSize();
		log.info(String.format("Start submitting %d batches", listOfFiles.length));
		for (File fd : listOfFiles) {
			es.execute(new AnnotConverter(batchSetId, fd.getName(), log, annotConvertOutputDataFile,  revampedCPEUtil));
		}		
		es.shutdown();
		try {
			boolean finished = es.awaitTermination(7, TimeUnit.DAYS);
			annotConvertOutputDataFile.flush();
			annotConvertOutputDataFile.close();
			log.info("All batches finished");
		} catch(Exception e) {
			e.printStackTrace();
		}
	}

	private static void initialize(String[] args) {
		if (args.length>0) {
			batchSetId = args[0];
		} else {
			batchSetId = "batchSet001";
		}

		try {
			log = new ICRISLogger(batchSetId,"annotConvertBatches");
			revampedCPEUtil = new CPEUtil("revamped.server.conf", log);
			log.info("Connected to P8 Domain "+ revampedCPEUtil.getDomain().get_Name());
			String annotConvertOutputFilePath = "." + File.separator + "data" + File.separator + "annotConvertOutput" + File.separator + batchSetId +".dat";
			Files.deleteIfExists(Paths.get(annotConvertOutputFilePath));
			annotConvertOutputDataFile = new FileOutputStream(annotConvertOutputFilePath, true);
		} catch (ICRISException e) {
			if (e.exceptionCode.equals(ICRISException.ExceptionCodeValue.CPE_CONFIG_FILE_NOT_FOUND)) {
				log.error(e.getMessage());				
			} else if (e.exceptionCode.equals(ICRISException.ExceptionCodeValue.CPE_CONFIG_FILE_CANNOT_BE_OPENED)) {
				log.error(e.getMessage());				
			} 
		} catch (Exception e) {
			log.error("unhandled Exception");
			log.error(e.toString());
		}		
	}

}
