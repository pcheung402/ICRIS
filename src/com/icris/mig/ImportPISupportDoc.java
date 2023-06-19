package com.icris.mig;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.LinkOption;
import java.io.FileFilter;
import java.sql.CallableStatement;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.concurrent.*;
import javax.activation.MimetypesFileTypeMap;
import com.icris.util.*;
import com.filenet.api.collection.ContentElementList;
import com.filenet.api.constants.AutoClassify;
import com.filenet.api.constants.CheckinType;
import com.filenet.api.constants.RefreshMode;
import com.filenet.api.core.ContentTransfer;
import com.filenet.api.core.Document;
import com.filenet.api.core.Factory;
import com.filenet.api.exception.EngineRuntimeException;
import com.filenet.api.exception.ExceptionCode;
import com.filenet.api.util.UserContext;
import com.filenet.api.util.Id;
import com.filenet.api.property.*;

public class ImportPISupportDoc {
	static 	String batchSetId;
	static ICRISLogger log;
	static CPEUtil revampedCPEUtil;
	static FileOutputStream importPISupportDocOutputDataFile;
	static String JDBCURL ;
	static java.sql.Connection conn;
	static String dbuser;
	static String dbpassword; 
	static String queryString;
	static String baseDirPath;
	static MimetypesFileTypeMap mimetypesFileTypeMap = new MimetypesFileTypeMap();
	

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		initialize(args);
		try {
			conn = DriverManager.getConnection(JDBCURL, dbuser, dbpassword);
			PreparedStatement queryStatement = conn.prepareStatement(queryString,  ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);			
			ResultSet rs = queryStatement.executeQuery();
			while(rs.next()) {
				String order_no = rs.getNString("ORDER_NO");
				Integer doc_seq_no = rs.getInt("DOC_SEQ_NO");
				String doc_type = rs.getNString("DOC_TYPE");
				Hashtable<String, Object> piSuportDocProps  = new Hashtable<String, Object>();
				piSuportDocProps.put("ORDER_NO", order_no);
				piSuportDocProps.put("DOC_SEQ_NO", doc_seq_no);
				piSuportDocProps.put("DOC_TYPE", doc_type);

				
				File piSupportDocFolderU = new File(baseDirPath + File.separator + order_no + File.separator + doc_type.toUpperCase());
				File piSupportDocFolderL = new File(baseDirPath + File.separator + order_no + File.separator + doc_type.toLowerCase());
				
				Boolean docFolderExist = Boolean.TRUE;
				if(piSupportDocFolderU.exists()) {	
					piSuportDocProps.put("SUPPORT_DOC_FOLDER",piSupportDocFolderU);					
					piSuportDocProps.put("LTST_VER", "1");
					piSuportDocProps.put("PRT_VW_IND", "V");					
				} else if(piSupportDocFolderL.exists()) {	
					piSuportDocProps.put("SUPPORT_DOC_FOLDER",piSupportDocFolderL);					
					piSuportDocProps.put("LTST_VER", "1");
					piSuportDocProps.put("PRT_VW_IND", "V");					
				} else {
					docFolderExist = Boolean.FALSE;
					log.error(String.format("%s is not found", baseDirPath + File.separator + order_no + File.separator + doc_type.toUpperCase()));
				}
				
				if (docFolderExist) {
					createPISupportDoc(piSuportDocProps);
//					System.out.println(piSuportDocProps.toString());
				}
				
			}
			queryStatement.close();
			log.info("finished");
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}/* catch (IOException e) {
			
		}*/ 
	}
	
	private static void initialize(String[] args) {
		if (args.length>0) {
			batchSetId = args[0];
		} else {
			batchSetId = "batchSet001";
		}
		
		try {
			java.util.Properties props = new java.util.Properties();
			props.load(new FileInputStream("config/oprdb.conf"));
			queryString = props.getProperty("PISupportDocQuery");
			JDBCURL = "jdbc:oracle:thin:@//"+ props.getProperty("OPRServer") + ":" + props.getProperty("OPRPort") + "/" + props.getProperty("OPRDatabase");
			dbuser = props.getProperty("OPRUser");
			dbpassword = props.getProperty("OPRPassword");
			baseDirPath = props.getProperty("PISupportDOCBaseDir");
			log = new ICRISLogger(batchSetId,"importBatches_PI_Support");
			revampedCPEUtil = new CPEUtil("revamped.server.conf", log);
			log.info("Connected to P8 Domain "+ revampedCPEUtil.getDomain().get_Name()+ ";" +revampedCPEUtil.getObjectStore().get_Name());
			String importPISupportDocOutputFilePath = "." + File.separator + "data" + File.separator + "importPISupportDocOutput_PI_Reg" + File.separator + batchSetId +".dat";
			Files.deleteIfExists(Paths.get(importPISupportDocOutputFilePath));
			importPISupportDocOutputDataFile = new FileOutputStream(importPISupportDocOutputFilePath, true);
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
	
	private static void createPISupportDoc(Hashtable piSuportDocProps) throws IOException, SQLException {
		CallableStatement callableStatement = conn.prepareCall("{CALL GEN_BARCODE(?)}");
		callableStatement.registerOutParameter(1, Types.VARCHAR);
		callableStatement.execute();
		String doc_barcode = callableStatement.getNString(1);
		callableStatement.close();
		
		String order_no = (String)piSuportDocProps.get("ORDER_NO");
		Integer doc_seq_no = (Integer)piSuportDocProps.get("DOC_SEQ_NO");
		String doc_type = (String)piSuportDocProps.get("DOC_TYPE");
		String latest_ver = (String)piSuportDocProps.get("LTST_VER");
		String print_view_ind = (String)piSuportDocProps.get("PRT_VW_IND");
		
		ContentElementList piSupportDocContentList = Factory.ContentElement.createList();
		
//		System.out.println(((File)piSuportDocProps.get("SUPPORT_DOC_FOLDER")).getPath());
		File piSupprtDocFolder = (File)piSuportDocProps.get("SUPPORT_DOC_FOLDER");
		
		File[] piSupportFiles = piSupprtDocFolder.listFiles(new FileFilter(){
			
			public boolean accept(File file) {
				if(file.isFile())
					return true;
				else
					return false;
			}			
		});
		

		if(piSupportFiles.length > 0) {
			for(File piSupportFile : piSupportFiles) {
				ContentTransfer  ct = Factory.ContentTransfer.createInstance();
				ct.setCaptureSource(new FileInputStream(piSupportFile));
				ct.set_ContentType(mimetypesFileTypeMap.getContentType(piSupportFile));
				ct.set_RetrievalName(piSupportFile.getName());
				piSupportDocContentList.add(ct);			
			}
			Document piSupportDoc= Factory.Document.createInstance(revampedCPEUtil.getObjectStore(), "ICRIS_Trans_Doc");
			piSupportDoc.getProperties().putValue("DocumentTitle", String.format("%s_%d", order_no, doc_seq_no ));
			piSupportDoc.getProperties().putValue("StorageAreaFlag", "0");
			piSupportDoc.getProperties().putValue("DOC_BARCODE", doc_barcode);
			piSupportDoc.getProperties().putValue("LTST_VER", latest_ver);
			piSupportDoc.getProperties().putValue("PRT_VW_IND", print_view_ind);
			
			switch (doc_type) {
			case "I":
				piSupportDoc.getProperties().putValue("DOC_TYPE", "ID");
				break;
			case "A":
				piSupportDoc.getProperties().putValue("DOC_TYPE", "AL");
				break;
			case "O":
				piSupportDoc.getProperties().putValue("DOC_TYPE", "SD");
				break;		
			default:
			}
			
			piSupportDoc.getProperties().putValue("DOC_TYPE", doc_type);
			piSupportDoc.save(RefreshMode.REFRESH);
			piSupportDoc.set_ContentElements(piSupportDocContentList);
			piSupportDoc.checkin(AutoClassify.DO_NOT_AUTO_CLASSIFY, CheckinType.MAJOR_VERSION);
			
			Id guid = piSupportDoc.getProperties().getIdValue("Id");
			piSupportDoc.save(RefreshMode.REFRESH);	
			importPISupportDocOutputDataFile.write(String.format("%s,%d,%s,%s,%s\n",order_no,doc_seq_no, doc_type,doc_barcode,guid.toString()).getBytes());
		} else {
			log.error(String.format("Folder %s is empty", piSupprtDocFolder.getPath()));
		}
	}

}
