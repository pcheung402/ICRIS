package com.icris.mig;

import org.apache.log4j.*;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
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


public class MarkDelete {
	static String batchSetId;
	static ICRISLogger log;
//	static ReplicateManager bmManager;
	static CPEUtil revampedCPEUtil;
	static FileOutputStream markDeleteOutputDataFile;
	static Boolean isByDocid = false;
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		initialize(args);
		ExecutorService es = Executors.newCachedThreadPool();
		File folder = new File("data/markdeletebatches/batchSets/" + batchSetId);
		File[] listOfFiles = folder.listFiles();
		log.info(String.format("Start submiiting %d  batches", listOfFiles.length));
		for (File fd : listOfFiles) {
			try {
				MarkDeleteManager markDeleteManager = new MarkDeleteManager(batchSetId, fd.getName(), log, markDeleteOutputDataFile, revampedCPEUtil, isByDocid);
				es.execute(markDeleteManager);
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
			} catch (IOException e) {
				log.error(e.getMessage());
			}
		}		
		es.shutdown();
		try {
			boolean finished = es.awaitTermination(1, TimeUnit.DAYS);
			markDeleteOutputDataFile.flush();
			markDeleteOutputDataFile.close();
			log.info("All batches finished");
		} catch (IOException e) {
			
		} catch(Exception e) {
			
		}
//		Thread bmThread = new Thread(new BulkMoverManager(batchSetId, log));
//		bmThread.start();

	}

	private static void initialize(String[] args) {
		if (args.length>0) {
			batchSetId = args[0];
		} else {
			batchSetId = "markdelete_001";
		}
		if (args.length > 1 && args[1].equals("-d")) {
			isByDocid = true;
		}

		try {
			log = new ICRISLogger(batchSetId,"markDeleteBatches");
			revampedCPEUtil = new CPEUtil("uat.server.conf", log);
			log.info("Connected to P8 Domain "+ revampedCPEUtil.getDomain().get_Name());
			String markDeleteOutputFilePath = "." + File.separator + "data" + File.separator + "markDeleteOutput" + File.separator + batchSetId +".dat";
			Files.deleteIfExists(Paths.get(markDeleteOutputFilePath));
			markDeleteOutputDataFile = new FileOutputStream(markDeleteOutputFilePath, true);

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
