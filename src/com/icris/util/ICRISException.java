package com.icris.util;

public class ICRISException extends Exception {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private String exceptionMessage;
	public enum ExceptionCodeValue {
		CPE_CONFIG_FILE_NOT_FOUND,
		CPE_CONFIG_FILE_CANNOT_BE_OPENED,
		CPE_CONFIG_FILE_ILLEGAL_ARGUMENT,
		CPE_URI_INVALID,
		CPE_USNAME_PASSWORD_INVALID,
		CPE_INVALID_OS_NAME,
		CPE_INVALID_SP_NAME,
		CPE_INVALID_SA_NAME,
		CPE_SA_UNAVAILABLE,
		BM_LOG4J_CONF_NOT_FOUND,
		BM_LOAD_LOG4J_CONFIG_ERROR,
		BM_LOAD_BATCH_SET_CONFIG_ERROR,
		BM_BATCH_SET_DATA_ERROR,
		BM_BATCH_NOT_UNIQUE,
		RPL_WORM_CLASS_NOT_FOUND,
		CB_UNABLE_TO_WRITE_BATCH_SET_DATA_FILE
	};
	
	public ExceptionCodeValue exceptionCode;

	public ICRISException(ExceptionCodeValue exceptionCode, String exceptionMessage, Exception e) {
		super (exceptionMessage, e);
		handleException(exceptionCode, exceptionMessage);
	}
	
	public ICRISException(ExceptionCodeValue exceptionCode, String exceptionMessage) {
		super (exceptionMessage);
		handleException(exceptionCode, exceptionMessage);
	}
	
	void handleException(ExceptionCodeValue exceptionCode, String exceptionMessage) {
		this.exceptionCode = exceptionCode;
		this.exceptionMessage = exceptionMessage;
		switch (exceptionCode) {
		case CPE_CONFIG_FILE_NOT_FOUND:
			break;
		case CPE_CONFIG_FILE_CANNOT_BE_OPENED:
			break;
		case CPE_CONFIG_FILE_ILLEGAL_ARGUMENT:
			break;
		case CPE_USNAME_PASSWORD_INVALID:
			break;
		case CPE_URI_INVALID:
			break;
		case CPE_INVALID_OS_NAME:
			break;
		case CPE_INVALID_SP_NAME:
			break;
		case CPE_INVALID_SA_NAME:
			break;
		case CPE_SA_UNAVAILABLE:
			break;
		case BM_LOG4J_CONF_NOT_FOUND:
			break;
		case BM_LOAD_LOG4J_CONFIG_ERROR:
			break;
		case BM_LOAD_BATCH_SET_CONFIG_ERROR:
			break;
		case BM_BATCH_SET_DATA_ERROR:
			break;
		case BM_BATCH_NOT_UNIQUE:
			break;
		case CB_UNABLE_TO_WRITE_BATCH_SET_DATA_FILE:
			break;
		default:
		}
		System.out.println(exceptionMessage);
	}
	
	public String getMessage() {
		return this.exceptionMessage;
	}
}
