package eu.gloria.rt.worker.offshore.talon.sch;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

import eu.gloria.rt.catalogue.Observer;
import eu.gloria.rti.sch.core.ObservingPlan;
import eu.gloria.rti.sch.core.plan.instruction.CameraSettings;
import eu.gloria.rti.sch.core.plan.instruction.Expose;
import eu.gloria.rti.sch.core.plan.instruction.Target;

public class SchGeneratorContext {
	
	private ObservingPlan op;
	private HashMap<String, String> filterMappingGloria2TALON;
	
	private Observer observer;
	private CameraSettings cameraSettings;
	private Expose expose;
	private Target target;
	
	private Date scheduleDateIni;
	private Date scheduleDateEnd;
	
	private List<String> output;
	private StringBuffer currentOuput;
	private String talonImgsPath;
	
	public SchGeneratorContext(){
		
		filterMappingGloria2TALON = new HashMap<String, String>();
		output = new ArrayList<String>();
	}
	
	/**
	 * FilterMapping:
	 * @param filterMapping GLORIA_FILTER1=TALON_FILTER1;GLORIA_FILTER2=TALON_FILTER2....
	 */
	public void loadFilterMapping(String filterMapping){
		
		filterMappingGloria2TALON.clear();
		
		if (filterMapping != null){
			
			String[] pairs = filterMapping.split(";");
			for (int pair = 0 ; pair < pairs.length; pair++){
				
				String[] values = pairs[pair].split("=");
				filterMappingGloria2TALON.put(values[0], values[1]);
			}
			
		}
	}
	
	public ObservingPlan getOp() {
		return op;
	}
	public void setOp(ObservingPlan op) {
		this.op = op;
	}
	public HashMap<String, String> getFilterMappingGloria2TALON() {
		return filterMappingGloria2TALON;
	}
	public void setFilterMappingGloria2TALON(
			HashMap<String, String> filterMappingGloria2TALON) {
		this.filterMappingGloria2TALON = filterMappingGloria2TALON;
	}
	public Observer getObserver() {
		return observer;
	}
	public void setObserver(Observer observer) {
		this.observer = observer;
	}
	public CameraSettings getCameraSettings() {
		return cameraSettings;
	}
	public void setCameraSettings(CameraSettings cameraSettings) {
		this.cameraSettings = cameraSettings;
	}
	public Expose getExpose() {
		return expose;
	}
	public void setExpose(Expose expose) {
		this.expose = expose;
	}
	public Target getTarget() {
		return target;
	}
	public void setTarget(Target target) {
		this.target = target;
	}
	public List<String> getOutput() {
		return output;
	}
	public void setOutput(List<String> output) {
		this.output = output;
	}

	public StringBuffer getCurrentOuput() {
		return currentOuput;
	}

	public void setCurrentOuput(StringBuffer currentOuput) {
		this.currentOuput = currentOuput;
	}

	public Date getScheduleDateIni() {
		return scheduleDateIni;
	}

	public void setScheduleDateIni(Date scheduleDateIni) {
		this.scheduleDateIni = scheduleDateIni;
	}

	public Date getScheduleDateEnd() {
		return scheduleDateEnd;
	}

	public void setScheduleDateEnd(Date scheduleDateEnd) {
		this.scheduleDateEnd = scheduleDateEnd;
	}

	public String getTalonImgsPath() {
		return talonImgsPath;
	}

	public void setTalonImgsPath(String talonImgsPath) {
		this.talonImgsPath = talonImgsPath;
	}

}
