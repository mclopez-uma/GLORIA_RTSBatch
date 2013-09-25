package eu.gloria.rt.worker.offshore.talon.telrun;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Container of the telrun.sls content file.
 * 
 * @author jcabello
 *
 */
public class TelrunDB {

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		
		TelrunDB db = new TelrunDB(new File("c:\\dummy\\telrun.sls"));

	}
	
	private boolean opened = false;
	private File file;
	private FileReader fr = null;
	private BufferedReader bf = null;
	private List<TelrunEntry> db = new ArrayList<TelrunEntry>();
	
	
	/**
	 * Constructor
	 * @param file telrun.sls file
	 * @throws Exception In error case
	 */
	public TelrunDB(File file) throws Exception{
		this.file = file;
		
		try{
			processStart();
		}finally{
			processEnd();
		}
	}
	
	public void clear(){
		
		for (int x = 0; x < db.size(); x++){
			db.get(x).clear();
		}
		
		db.clear();
	}
	
	public List<TelrunEntry> getEntries(String opUuid) throws Exception{
		
		List<TelrunEntry> result = new ArrayList<TelrunEntry>();
		
		if (db != null){
			for (int x = 0; x < db.size(); x++){
				if (opUuid.equals(db.get(x).getUUID())){
					result.add(db.get(x));
				}
			}
		}
		
		return result;
			
	}
	
	public boolean isOpExecuted(String opUuid)throws Exception{
		
		boolean result = true;
		
		List<TelrunEntry> entries = getEntries(opUuid);
		if (entries != null && entries.size()>0){
			for (int x = 0;x < entries.size(); x++){
				if (!"D".equals(entries.get(x).getState())){
					result = false;
					break;
				}
			}
		}else{
			result = false;
		}
		
		return result;
	}
	
	private void processStart() throws Exception{
		
		try {
			fr = new FileReader(file);
			bf = new BufferedReader(fr);
		} catch (Exception ex) {
			ex.printStackTrace();
			opened = false;
			throw ex;
		}
		
		load();
		
		opened = true;
		
	}
	
	private void processEnd(){
		
		opened = false;
		
		if (bf != null){
			try {
				bf.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	private void load() throws IOException{
		
		String line = null;
		TelrunEntry current = null;
		while ((line = bf.readLine())!=null) {
			
			if (line.startsWith(" 0")){ //New entry
				
				if (current != null){
					if (current.isReady()){
						db.add(current);
						//System.out.println(current.toString() + "\n");
					}else{
						//LOG show entry content
					}
					current = null;
				}
				
				current = new TelrunEntry();
			}
			
			current.processLine(line);
		}
		
		if (current!= null){
			if (current.isReady()){
				db.add(current);
				
				//System.out.println(current.toString() + "\n");
				
			}else{
				//LOG show entry content
			}
		}
		
	}

}
