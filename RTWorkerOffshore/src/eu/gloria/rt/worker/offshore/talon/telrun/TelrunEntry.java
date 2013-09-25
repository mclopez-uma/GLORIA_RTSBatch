package eu.gloria.rt.worker.offshore.talon.telrun;

import java.util.HashMap;


/**
 * Telrun entry wrapper.
 * 
 * @author jcabello
 *
 */
public class TelrunEntry {
	
	private HashMap<String, String> params;
	
	/**
	 * Constructor
	 */
	public TelrunEntry(){
		params  = new HashMap<String, String>();
	}
	
	public void clear(){
		params.clear();
	}
	
	/**
	 * Adds to the instance the information contained by the String line
	 * @param line Telrun.sls content line.
	 */
	public void processLine(String line){
		
		String name = line.substring(2,20).trim();
		String value = line.substring(22).trim();
		
		params.put(name, value);
	}
	
	/**
	 * Returns true if the mandatory information is available.
	 * @return boolean.
	 */
	public boolean isReady(){
		return true;
	}
	
	/**
	 * Internal access method.
	 * @param name Telrun Parameter name
	 * @return value
	 * @throws Exception If the parameter does not exist.
	 */
	private String getParam(String name) throws Exception{
		String result = params.get(name);
		if (result == null) throw new Exception("Unexisting param: " + name);
		return result;
	}
	
	/**
	 * Access to the state.
	 * @return State.
	 * @throws Exception In error case
	 */
	public String getState() throws Exception{
		return getParam("status");
	}
	
	/**
	 * Access to the title.
	 * @return title.
	 * @throws Exception In error case
	 */
	public String getTitle() throws Exception{
		return getParam("title");
	}
	
	/**
	 * Access to the image.
	 * @return image path.
	 * @throws Exception In error case
	 */
	public String getImage() throws Exception{
		return getParam("pathname");
	}
	
	/**
	 * Access to the comment.
	 * @return comment.
	 * @throws Exception In error case
	 */
	public String getComment() throws Exception{
		return getParam("comment");
	}
	
	
	/**
	 * Access to the observer.
	 * @return observer.
	 * @throws Exception In error case
	 */
	public String getObserver() throws Exception{
		return getParam("observer");
	}
	
	public String getUUID() throws Exception{
		return getParam("schedfn");
	}
	

	public String toString(){
		StringBuilder sb = new StringBuilder();
		sb.append("[");
		for (String  name : params.keySet()) {
			sb.append(name).append("=").append(params.get(name));
			sb.append(", ");
		}
		sb.append("]");
		
		return sb.toString();
	}

}
