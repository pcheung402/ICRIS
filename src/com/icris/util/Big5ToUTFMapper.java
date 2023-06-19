package com.icris.util;
import java.io.PrintStream;
import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.UnsupportedEncodingException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.UnmappableCharacterException;
import java.nio.charset.MalformedInputException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.Map.Entry;

import com.filenet.api.core.Document;
import com.filenet.api.core.Annotation;
import com.google.gson.*;



public class Big5ToUTFMapper {
	private enum MapResultCode {
		StandardCharacter,
		HKSCS,
		MalformedInput,
		UnmappableCharacter,
		CharacterCoding,
		UnsupportedEncoding,
		Big5UTFNotMatch,
		UserDefinedCHaracter
	}


	private class Big5ToUTFMappingItem {
		private Integer	big5Code;
		private Integer	utf16Code;
		private String	big5Char;
		private String	utf16Char;
		private MapResultCode mapResultCode;
		
		Big5ToUTFMappingItem(Integer big5Code, Integer utf16Code, String big5Char, String utf16Char, MapResultCode errorCode){
			this.big5Code = big5Code;
			this.utf16Code = utf16Code;
			this.big5Char = big5Char;
			this.utf16Char = utf16Char;
			this.mapResultCode = errorCode;
		}
	}
	
	private String big5CharSetName;
	private String utfCharSetName;
	private Map<Integer, Big5ToUTFMappingItem> big5ToUTFMap;
	private ICRISLogger log;
	private String questionMarkUTFCode = "003f";
//	private Map<Integer, Integer> hkscsCodeMap;

	
	public Big5ToUTFMapper(ICRISLogger log) {
		this("Big5-HKSCS","UTF-16BE", log);
	}
	
	public Big5ToUTFMapper(String big5CharSetName, String utfCharSetName, ICRISLogger log) {
		this.log = log;
		this.big5CharSetName = big5CharSetName;
		this.utfCharSetName = utfCharSetName;
		big5ToUTFMap = new TreeMap<Integer, Big5ToUTFMappingItem>();
		createBig5ToUTFMap(Integer.parseInt("0000", 16) ,Integer.parseInt("007f", 16), "ASCII characters");
    	createBig5ToUTFMap(Integer.parseInt("8140", 16) ,Integer.parseInt("a0fe", 16), "Reserved for user-defined characters");
    	createBig5ToUTFMap(Integer.parseInt("a140", 16) ,Integer.parseInt("a3bf", 16), "Graphical characters");
    	createBig5ToUTFMap(Integer.parseInt("a3c0", 16) ,Integer.parseInt("a3fe", 16), "Reserved for user-defined characters");
    	createBig5ToUTFMap(Integer.parseInt("a440", 16) ,Integer.parseInt("c670", 16), "Frequently used characters");
    	createBig5ToUTFMap(Integer.parseInt("c6a1", 16) ,Integer.parseInt("c8fe", 16), "Reserved for user-defined characters");
    	createBig5ToUTFMap(Integer.parseInt("c940", 16) ,Integer.parseInt("f9d5", 16), "Less frequently used characters");
    	
    	createHKSCSCodeMap();
    	createUserDefinedCodeMap();
    	

	}
	
	private void createHKSCSCodeMap() {
		
		try {
			Gson gson = new Gson();
			JsonElement json = gson.fromJson(new FileReader("." + File.separator + "config" + File.separator + "HKSCS.json"), JsonElement.class);
			JsonArray jArray = json.getAsJsonArray();
			for (JsonElement jElement : jArray) {
				String[] H_Source_Value = jElement.getAsJsonObject().get("H-Source").getAsString().split("-");
				Integer big5Code = Integer.parseInt(H_Source_Value[1], 16);
				Integer utf16Code = Integer.parseInt(jElement.getAsJsonObject().get("codepoint").getAsString(), 16);
				String strBig5 =  jElement.getAsJsonObject().get("char").getAsString();
				if("H".contentEquals(H_Source_Value[0])) {
					big5ToUTFMap.put(big5Code, new Big5ToUTFMappingItem(big5Code, utf16Code, strBig5, "", MapResultCode.HKSCS));
				} else {
					if(big5ToUTFMap.get(big5Code)==null) {
						big5ToUTFMap.put(big5Code, new Big5ToUTFMappingItem(big5Code, utf16Code, strBig5, "", MapResultCode.HKSCS));
					}
				}				
			}
			
			big5ToUTFMap.remove(0x95b2);
			big5ToUTFMap.remove(0x8aac);
			big5ToUTFMap.remove(0x92ed);
			big5ToUTFMap.remove(0x9c47);
			

		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
	
	
	private void createUserDefinedCodeMap() {
		String userDefinedCodeMapFilePath = "." + File.separator + "config" + File.separator + "UserDefinedCS.json";
		
		try {
			Gson gson = new Gson();
			File userDefinedCodeMapFile = new File(userDefinedCodeMapFilePath);
			if(userDefinedCodeMapFile.exists()&&!userDefinedCodeMapFile.isDirectory()) {
				JsonElement json = gson.fromJson(new FileReader(userDefinedCodeMapFilePath), JsonElement.class);
				JsonArray jArray = json.getAsJsonArray();
				for (JsonElement jElement : jArray) {
					String[] H_Source_Value = jElement.getAsJsonObject().get("H-Source").getAsString().split("-");
					Integer big5Code = Integer.parseInt(H_Source_Value[1], 16);
					Integer utf16Code = Integer.parseInt(jElement.getAsJsonObject().get("codepoint").getAsString(), 16);
					String strBig5 =  jElement.getAsJsonObject().get("char").getAsString();
					if("H".contentEquals(H_Source_Value[0])) {
						big5ToUTFMap.put(big5Code, new Big5ToUTFMappingItem(big5Code, utf16Code, strBig5, "", MapResultCode.UserDefinedCHaracter));
					} else {
						if(big5ToUTFMap.get(big5Code)==null) {
							big5ToUTFMap.put(big5Code, new Big5ToUTFMappingItem(big5Code, utf16Code, strBig5, "", MapResultCode.UserDefinedCHaracter));
						}
					}				
				}			
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
    
    private void createBig5ToUTFMap(Integer startBig5Code, Integer endBig5Code, String codeRangeDescription) {
//    	System.out.printf("%04x, %04x, %s\n",startBig5Code, endBig5Code, codeRangeDescription);
    	for (Integer i=startBig5Code;i<=endBig5Code;++i) {
			addBig5ToUTFMapEntry(i);
    	}
    }
    
    private void addBig5ToUTFMapEntry(Integer big5Code){
    	String strBig5 = null;
    	String strUTF16 = null;
    	byte[] big5_bytearray = new byte[2];
    	big5_bytearray[1] = (byte)(big5Code&0xff);
    	big5_bytearray[0] =(byte)((big5Code>>8)&0xff);
    	ByteBuffer big5bb = ByteBuffer.wrap(big5_bytearray);

    	try {
        	strBig5 = new String(big5_bytearray,this.big5CharSetName);
	    	if(big5Code>=0x0000&&big5Code<=0x007f) {
	    		if(big5Code==0x000d) {
	    			big5ToUTFMap.put(big5Code, new Big5ToUTFMappingItem(big5Code, big5Code ,"<CR>", "<CR>", MapResultCode.StandardCharacter));
	    		} else if (big5Code==0x000a) {
	    			big5ToUTFMap.put(big5Code, new Big5ToUTFMappingItem(big5Code, big5Code ,"<LF>", "<LF>", MapResultCode.StandardCharacter));
	    		} else {
	    			big5ToUTFMap.put(big5Code, new Big5ToUTFMappingItem(big5Code, big5Code ,strBig5.substring(1), strBig5.substring(1), MapResultCode.StandardCharacter));
	    		}
	    	} else if (big5Code>=0x0080&&big5Code<=0x00ff) {
	    		
	    	} else {
				Charset charsetBig5 = Charset.forName(this.big5CharSetName);    	
				CharsetDecoder big5Decoder = charsetBig5.newDecoder();
				Charset charsetUtf16 = Charset.forName(utfCharSetName);
				CharsetEncoder utf16Encoder = charsetUtf16.newEncoder();			
				CharBuffer cb = big5Decoder.decode(big5bb);
				ByteBuffer utf16bb = utf16Encoder.encode(cb);
				byte[] utf16_bytearray = utf16bb.array();
				Integer utf16Code=(int)(utf16_bytearray[0] & 0xFF);
				if (utf16_bytearray.length > 1) {
					utf16Code = (int)(utf16Code << 8 | (utf16_bytearray[1] & 0xFF));
				}
				if (utf16_bytearray.length > 2) {
					utf16Code = (int)(utf16Code << 8 | (utf16_bytearray[2] & 0xFF));
				}
				if (utf16_bytearray.length > 3) {
					utf16Code = (int)(utf16Code << 8 | (utf16_bytearray[3] & 0xFF));
				}

		    	strUTF16 = new String(utf16_bytearray,utfCharSetName);
		    	
		    	if(strBig5.equals(strUTF16)) {
		    		big5ToUTFMap.put(big5Code, new Big5ToUTFMappingItem(big5Code, utf16Code, strBig5, strUTF16, MapResultCode.StandardCharacter));
		    	} else {
		    		big5ToUTFMap.put(big5Code, new Big5ToUTFMappingItem(big5Code, utf16Code,strBig5, strUTF16, MapResultCode.Big5UTFNotMatch));

		    	}
	    	}
    	} catch (MalformedInputException e) {
	    	Big5ToUTFMappingItem big5ToUTF16MappingEntry = new Big5ToUTFMappingItem(big5Code,Integer.parseInt("0000", 16), strBig5, strUTF16, MapResultCode.MalformedInput);
	    	big5ToUTFMap.put(big5Code, big5ToUTF16MappingEntry);

    	} catch (UnmappableCharacterException e) {
	    	Big5ToUTFMappingItem big5ToUTF16MappingEntry = new Big5ToUTFMappingItem(big5Code,Integer.parseInt("0000", 16), strBig5, strUTF16, MapResultCode.UnmappableCharacter);
	    	big5ToUTFMap.put(big5Code, big5ToUTF16MappingEntry);
    	} catch (CharacterCodingException e) {
	    	Big5ToUTFMappingItem big5ToUTF16MappingEntry = new Big5ToUTFMappingItem(big5Code,Integer.parseInt("0000", 16), strBig5, strUTF16, MapResultCode.CharacterCoding);
	    	big5ToUTFMap.put(big5Code, big5ToUTF16MappingEntry);
    	} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
	    	Big5ToUTFMappingItem big5ToUTF16MappingEntry = new Big5ToUTFMappingItem(big5Code,Integer.parseInt("0000", 16), strBig5, strUTF16, MapResultCode.UnsupportedEncoding);
	    	big5ToUTFMap.put(big5Code, big5ToUTF16MappingEntry);

		}
    }
    
    public void printMap() {
    	BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(System.out));
    	this.printMap(bw);
    }
    
    public void printMap(BufferedWriter bw){
    	for(Entry<Integer, Big5ToUTFMappingItem> entry : big5ToUTFMap.entrySet()) {
    		try {
    			bw.write(String.format("%04x, %04x, %s, %s, %s\n", entry.getValue().big5Code, entry.getValue().utf16Code, entry.getValue().big5Char, entry.getValue().utf16Char, entry.getValue().mapResultCode.name()));
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}    		
    	}
    }
    
    public void printMap(BufferedWriter bw_big5, BufferedWriter bw_utf){
    	for(Entry<Integer, Big5ToUTFMappingItem> entry : big5ToUTFMap.entrySet()) {
    		try {
    			bw_big5.write(String.format("%04x, %04x, %s, %s\n", entry.getValue().big5Code, entry.getValue().utf16Code, entry.getValue().big5Char, entry.getValue().mapResultCode.name()));
    			bw_utf.write(String.format("%04x, %04x, %s, %s\n", entry.getValue().big5Code, entry.getValue().utf16Code, entry.getValue().utf16Char, entry.getValue().mapResultCode.name()));
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}    		
    	}
    }
    
    public String convertFromBig5ToUTF(String strBig5, Document doc, Annotation annotOrig,  FileOutputStream ofs) throws ICRISException, IOException {
    	String retStr="";
    	Integer tempInt;
    	String tempStr=""; 
    	Boolean flag=false;
    	for(String s :strBig5.split("(?<=\\G.{4})")) {
        	tempInt = Integer.parseUnsignedInt(s, 16);
	        if(flag) {
	        	tempStr = tempStr + String.format("%02x",  Integer.parseUnsignedInt(s, 16));
	        	Big5ToUTFMappingItem mappingEntry = this.big5ToUTFMap.get(Integer.parseInt(tempStr, 16));
	        	retStr = retStr +convertSingleBig5ToUTF(mappingEntry, s, doc, annotOrig, ofs);
	        	flag = false;
	        } else {
	        	if(tempInt>=0x0080&&tempInt<=0x00ff) {
	    			tempStr = String.format("%02x", Integer.parseUnsignedInt(s, 16));
	    			flag=true;
	        	} else {
	        		tempStr="";
		        	Big5ToUTFMappingItem mappingEntry = this.big5ToUTFMap.get(Integer.parseInt(s, 16));
		        	retStr = retStr +convertSingleBig5ToUTF(mappingEntry, s, doc, annotOrig, ofs);
	        	}
	        }
    		
    	}
    	return retStr.toUpperCase();
    }
    
    public Integer  getUTFCode(String strBig5) {
    	return this.big5ToUTFMap.get(Integer.parseInt(strBig5, 16)).utf16Code;
    	
    }
    
    private String convertSingleBig5ToUTF(Big5ToUTFMappingItem mappingEntry, String s, Document doc, Annotation annotOrig,  FileOutputStream ofs) throws ICRISException, IOException {
    	com.filenet.api.property.Properties docProperties = doc.getProperties();
    	if (mappingEntry==null) {
//    		throw new ICRISException(ICRISException.ExceptionCodeValue.CC_INVALIDBIG5CODE,String.format("Invalid Big5 Code : %s", s));
//    		log.error(String.format("Invalid Big5 Code(%s) : %s,%s,%s,%10.0f,%d",s, doc.get_Id().toString(), annotOrig.get_Id().toString(),docProperties.getStringValue("DOC_BARCODE"), docProperties.getFloat64Value("F_DOCNUMBER"), annotOrig.get_AnnotatedContentElement()));
			ofs.write(String.format("%s,%s,%s,%10.0f,%d, Invalid Big5 Code(%s)\n",doc.get_Id().toString(), annotOrig.get_Id().toString(),docProperties.getStringValue("DOC_BARCODE"), docProperties.getFloat64Value("F_DOCNUMBER"), annotOrig.get_AnnotatedContentElement(), s).getBytes());
    		return questionMarkUTFCode;
    	}

    	if (mappingEntry.mapResultCode == mappingEntry.mapResultCode.StandardCharacter) {
    		return String.format("%04x", mappingEntry.utf16Code);
    	} else if (mappingEntry.mapResultCode == mappingEntry.mapResultCode.HKSCS) {
    		if (mappingEntry.utf16Code <= 0xffff) {
    			return  String.format("%04x", mappingEntry.utf16Code);
    		} else {
    			byte[] dummyBytes = String.format("%d",mappingEntry.utf16Code).getBytes();
    			String dummyStr="";
    			for (byte b:dummyBytes) {
    				dummyStr = dummyStr + String.format("%04x", b);
    			}
    			return String.format("00260023%s003B",dummyStr); 
    		}
    		
    	} else {
//    		throw new ICRISException(ICRISException.ExceptionCodeValue.CC_INVALIDBIG5CODE,String.format("Invalid Big5 Code : %s, %04x, %04x", mappingEntry.mapResultCode.name(), mappingEntry.big5Code, mappingEntry.utf16Code));
//    		log.error(String.format(String.format("Unmappable Big5 Code : %s, %04x, %04x, %s, %s, %10.0f", mappingEntry.mapResultCode.name(), mappingEntry.big5Code, mappingEntry.utf16Code, docProperties.getIdValue("Id").toString(), docProperties.getStringValue("DOC_BARCODE"), docProperties.getFloat64Value("F_DOCNUMBER"))));
    		ofs.write(String.format("%s,%s,%s,%10.0f,%d, Unmappable Big5 Code(%s, %04x, %04x)\n",doc.get_Id().toString(), annotOrig.get_Id().toString(),docProperties.getStringValue("DOC_BARCODE"), docProperties.getFloat64Value("F_DOCNUMBER"), annotOrig.get_AnnotatedContentElement(),mappingEntry.mapResultCode.name(), mappingEntry.big5Code, mappingEntry.utf16Code).getBytes());
    		return questionMarkUTFCode;
    	}    	
    }
}