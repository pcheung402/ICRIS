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


public class BulkMove_PI_Pend {
	static String batchSetId;
	static ICRISLogger log;
	static BulkMoverManager bmManager;
	static CPEUtil revampedCPEUtil;
	static FileOutputStream bulkMoveOutputDataFile;
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		HashMap<String, Double>  intialSASize, finalSASize;
		initialize(args);
		log.info("Connected to : " + revampedCPEUtil.getDomain().get_Name() + ";" +revampedCPEUtil.getObjectStore().get_Name());
//		System.out.println(revampedCPEUtil.getStorageArea("ICRIS_Tmplt_Doc").get_DisplayName());
		ExecutorService es = Executors.newCachedThreadPool();
		File folder = new File("data/bulkMoveBatches_PI_Pend/batchSets/" + batchSetId);
		File[] listOfFiles = folder.listFiles();
//		intialSASize = revampedCPEUtil.getStorageAreaSize();
		log.info(String.format("Start submitting %d batches", listOfFiles.length));
		for (File fd : listOfFiles) {
			es.execute(new BulkMoverManager_PI_Pend(batchSetId, fd.getName(), log, bulkMoveOutputDataFile,  revampedCPEUtil));
		}		
		es.shutdown();
		try {
			boolean finished = es.awaitTermination(7, TimeUnit.DAYS);
			bulkMoveOutputDataFile.flush();
			bulkMoveOutputDataFile.close();
			log.info("All batches finished");
//			log.info("Wait 5 minutes and calculate total storage moved in MB");
//			Thread.sleep(5*60*1000);
//			finalSASize = revampedCPEUtil.getStorageAreaSize();
//			
//			intialSASize.keySet().forEach((key)->{
//				log.info(String.format("Initial Storage Size: %.3f(MB), Final Storage Size: %.3f(MB), Total Moved : %.3f(MB)", intialSASize.get(key)/1024, finalSASize.get(key)/1024, (finalSASize.get(key) - intialSASize.get(key))/1024));				
//			});
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
			log = new ICRISLogger(batchSetId,"bulkMoveBatches_PI_Pend");
			revampedCPEUtil = new CPEUtil("revamped.server.conf", log);
			log.info("Connected to P8 Domain "+ revampedCPEUtil.getDomain().get_Name());
			String bulkMoveOutputFilePath = "." + File.separator + "data" + File.separator + "bulkMoveOutput_PI_Pend" + File.separator + batchSetId +".dat";
			Files.deleteIfExists(Paths.get(bulkMoveOutputFilePath));
			bulkMoveOutputDataFile = new FileOutputStream(bulkMoveOutputFilePath, true);
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
