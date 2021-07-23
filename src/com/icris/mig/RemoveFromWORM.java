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


public class RemoveFromWORM {
	static String batchSetId;
	static ICRISLogger log;
//	static ReplicateManager bmManager;
	static CPEUtil revampedCPEUtil;
	static FileOutputStream removeFromWormOutputDataFile;
	static Boolean isByDocid = false;
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		initialize(args);
		ExecutorService es = Executors.newCachedThreadPool();
		File folder = new File("data/removeWormBatches/batchSets/" + batchSetId);
		File[] listOfFiles = folder.listFiles();
		log.info(String.format("Start submiiting %d  batches", listOfFiles.length));
		for (File fd : listOfFiles) {
			try {
				RemoveFromWormManager removeFromWormManager = new RemoveFromWormManager(batchSetId, fd.getName(), log, removeFromWormOutputDataFile, revampedCPEUtil, isByDocid);
				es.execute(removeFromWormManager);
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
			removeFromWormOutputDataFile.flush();
			removeFromWormOutputDataFile.close();
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
			batchSetId = "batchSet001";
		}
		if (args.length > 1 && args[1].equals("-d")) {
			isByDocid = true;
		}

		try {
			log = new ICRISLogger(batchSetId,"removeWormBatches");
			revampedCPEUtil = new CPEUtil("revamped.server.conf", log);
			log.info("Connected to P8 Domain "+ revampedCPEUtil.getDomain().get_Name());
			String removeFromWormOutputFilePath = "." + File.separator + "data" + File.separator + "removeFromWormOutput" + File.separator + batchSetId +".dat";
			Files.deleteIfExists(Paths.get(removeFromWormOutputFilePath));
			removeFromWormOutputDataFile = new FileOutputStream(removeFromWormOutputFilePath, true);

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
