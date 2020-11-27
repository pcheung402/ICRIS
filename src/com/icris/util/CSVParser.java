package com.icris.util;
import java.util.ArrayList;
import java.util.HashMap;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.IOException;
public class CSVParser {
	enum LineParserState{
		INIT_STATE,
		RECEIVING_TOKEN_OPEN_DOUBLEQUOTE_RECEIVED,
		RECEIVING_TOKEN_NO_DOUBLEQUOTE_RECEIVED,
		EXPECTING_SEPARATOR	
	}
	
	LineParserState lineParserState;

    public String[] lineParser(String line) {
    	lineParserState = LineParserState.INIT_STATE;
    	String strBuffer="";
    	ArrayList<String> parsedLine = new ArrayList<String>();
    	char[] lineBytes = line.toCharArray();
    	for (char lineByte : lineBytes){
    		switch (lineParserState) {
    		case INIT_STATE:
    			if (lineByte == ','){
    				parsedLine.add(""); 
    				lineParserState = LineParserState.INIT_STATE;
    			} else if (lineByte == ' '){
    				lineParserState = LineParserState.INIT_STATE;
    			} else if (lineByte == '"'){
    				lineParserState = LineParserState.RECEIVING_TOKEN_OPEN_DOUBLEQUOTE_RECEIVED;
    			} else {
    				strBuffer += lineByte;
    				lineParserState = LineParserState.RECEIVING_TOKEN_NO_DOUBLEQUOTE_RECEIVED;
    			}
    			break;
    		case RECEIVING_TOKEN_OPEN_DOUBLEQUOTE_RECEIVED:
    			if (lineByte == '"'){
    				strBuffer = strBuffer.trim();
    				parsedLine.add(strBuffer);   					
    				strBuffer="";
    				lineParserState = LineParserState.EXPECTING_SEPARATOR;
    			} else {
    				strBuffer += lineByte;
    				lineParserState = LineParserState.RECEIVING_TOKEN_OPEN_DOUBLEQUOTE_RECEIVED;  
    			}
    			break;
    		case EXPECTING_SEPARATOR:
    			if (lineByte == ','){
    				lineParserState = LineParserState.INIT_STATE; 
    			} else {
    				lineParserState = LineParserState.EXPECTING_SEPARATOR;
    			}
    			break;
    		case RECEIVING_TOKEN_NO_DOUBLEQUOTE_RECEIVED:
    			if (lineByte == ','){
    				strBuffer = strBuffer.trim();
    				if (strBuffer.length()>0){
//    					System.out.println(lineParserState+";"+strBuffer);
        				parsedLine.add(strBuffer);   					
    				}
    				strBuffer="";
    				lineParserState = LineParserState.INIT_STATE; 
    			} else {
    				strBuffer += lineByte;
    				lineParserState = LineParserState.RECEIVING_TOKEN_NO_DOUBLEQUOTE_RECEIVED;
    			}
    			break;
    		default:	
    		}
    		
    	}    	
    	if (strBuffer.length() > 0) {
    		parsedLine.add(strBuffer);
    	} else {
    		parsedLine.add("");
    	}
    	return parsedLine.toArray(new String[parsedLine.size()]);
    }
    
    public ArrayList<HashMap<String,String>> parser(InputStream fis) {
//      URL url = CSVParser.class.getResource(path);
      BufferedReader br = null;
      ArrayList<HashMap<String,String>> result = new ArrayList<HashMap<String,String>>();
      try {
          
          br = new BufferedReader(new InputStreamReader(fis, "UTF8"));
         
          
          /** Parsing each line in the file */
          String line = "";
          
          //
          // read the header line
          //
          line =  br.readLine();
          if (line == null) return result;	//input file must contain least the header line;           
          String[] headers = lineParser(line);
          while ((line = br.readLine()) != null) {             
              /** Parse each line into values */
              String[] values = lineParser(line);
              HashMap<String,String> lineEntry = new HashMap<String,String>();
              for (int i=0; i<values.length; ++i){
              	if (i < headers.length){
              		lineEntry.put(headers[i], values[i]);
              	} else {
              		System.out.println("***"+i+"***"+values[i]);
              	}
              }
              /** Adding the lines to the array list */
              result.add(lineEntry);
          }
      }
      catch (Exception e) {
          /** Just display the error */
          e.printStackTrace();
      }
      finally {
          /** Closing the the stream */
          if (br != null) {
              try {
                  br.close();
              } catch (IOException e) {
                  e.printStackTrace();
              }
          }
      }
      return result;
    }
	
	public CSVParser() {
		// TODO Auto-generated constructor stub
	}

}
