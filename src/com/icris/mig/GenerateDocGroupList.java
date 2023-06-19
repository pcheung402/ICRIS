package com.icris.mig;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.icris.util.CSVParser;

public class GenerateDocGroupList {
	private static CSVParser csvParser;
	private static FileOutputStream bulkMoveOutputDataFile;
	private static String JDBCURL; 
	private static String dbuser;
	private static String dbpassword;
	private static java.sql.Connection conn;	
	public static void main(String[] args) throws IOException, SQLException {
		initialize(args);
		// TODO Auto-generated method stub
		BufferedReader reader;

		reader = new BufferedReader(new FileReader("./barcode.lst"));
		for (String line; (line = reader.readLine()) != null; ){// each line contains one DOC_GRP_NO
			String[] parsedLine = csvParser.lineParser(line);
			String doc_barcode = parsedLine[0];
			Integer doc_grp_no;			
			String queryString = "SELECT DOC_GRP_NO FROM CR_DOC_RLTSHP WHERE DOC_BARCODE=? AND LNK_TYP='8'";
			System.out.println(doc_barcode);
			PreparedStatement queryStatement = conn.prepareStatement(queryString,  ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
			queryStatement.setString(1, doc_barcode);
			ResultSet rs = queryStatement.executeQuery();
			
			if(rs.next()) {
				doc_grp_no = rs.getInt("DOC_GRP_NO");
				bulkMoveOutputDataFile.write(String.format("%d\n", doc_grp_no).getBytes());
			}
			rs.close();
			queryStatement.close();
			
		}
		
	}
	private static void initialize(String[] args) throws IOException, SQLException {
		csvParser = new CSVParser();
		bulkMoveOutputDataFile = new FileOutputStream("./doc_grp_no.lst", true);
		java.util.Properties props = new java.util.Properties();
		props.load(new FileInputStream("config/oprdb.conf"));
		JDBCURL = "jdbc:oracle:thin:@//"+ props.getProperty("OPRServer") + ":" + props.getProperty("OPRPort") + "/" + props.getProperty("OPRDatabase");
		dbuser = props.getProperty("OPRUser");
		dbpassword = props.getProperty("OPRPassword");
		conn = DriverManager.getConnection(JDBCURL, dbuser, dbpassword);
	}
}
