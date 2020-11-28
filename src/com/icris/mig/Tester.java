package com.icris.mig;

import java.io.IOException;

import com.filenet.api.core.ObjectStore;
import com.icris.util.ICRISLogger;
import com.icris.util.CPEUtil;
import com.icris.util.ICRISException;


public class Tester {
	static private CPEUtil revampedCPEUtil;
	static private ObjectStore objectStore = null;
	static private ICRISLogger log;
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		System.out.println("java.security.auth.login.config : "+ System.getProperty("java.security.auth.login.config"));
		try {
			/*
			 * Connect to CPE using CEUtil
			 * 
			 * Changes committed to branch icrisicris8
			 */
			log = new ICRISLogger("Test",null);
			revampedCPEUtil = new CPEUtil("revamped.server.conf", null);
			objectStore = revampedCPEUtil.getObjectStore();
			log.info("Connected to : " + revampedCPEUtil.getDomain().get_Name() + ";" +revampedCPEUtil.getObjectStore().get_Name());
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
		}
	}

}
