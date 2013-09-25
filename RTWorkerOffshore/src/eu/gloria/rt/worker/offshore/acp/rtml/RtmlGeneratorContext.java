package eu.gloria.rt.worker.offshore.acp.rtml;

import java.util.Date;
import java.util.HashMap;

import eu.gloria.rt.catalogue.Observer;
import eu.gloria.rt.entity.device.FilterType;
import eu.gloria.rti.sch.core.ObservingPlan;
import eu.gloria.rti.sch.core.plan.instruction.CameraSettings;
import eu.gloria.rti.sch.core.plan.instruction.Expose;
import eu.gloria.rti.sch.core.plan.instruction.Target;
import eu.gloria.tools.log.LogUtil;


public class RtmlGeneratorContext {
	
	private String projectName;
	private String requestUser;
	private String requestEmail;
	private String requestDesc;
	private String requestId;
	private Target target;
	private CameraSettings cameraSettings;
	private Expose expose;
	private StringBuffer output;
	private HashMap<String, String> filterMappingGloria2ACP;
	private ObservingPlan op;
	private Observer observer;
	private Date scheduleDateIni;
	private Date scheduleDateEnd;
	private RtmlSkyCondition skyCondition;
	
	public RtmlGeneratorContext(){
		filterMappingGloria2ACP = new HashMap<String, String>();
		skyCondition = RtmlSkyCondition.None;
	}
	
	/**
	 * FilterMapping:
	 * @param filterMapping GLORIA_FILTER1=ACP_FILTER1;GLORIA_FILTER2=ACP_FILTER2....
	 */
	public void loadFilterMapping(String filterMapping){
		
		filterMappingGloria2ACP.clear();
		
		if (filterMapping != null){
			
			String[] pairs = filterMapping.split(";");
			for (int pair = 0 ; pair < pairs.length; pair++){
				
				String[] values = pairs[pair].split("=");
				filterMappingGloria2ACP.put(values[0], values[1]);
			}
			
		}
	}
	
	public Target getTarget() {
		return target;
	}
	public void setTarget(Target target) {
		this.target = target;
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
	public String getRequestId() {
		return requestId;
	}
	public void setRequestId(String requestId) {
		this.requestId = requestId;
	}
	public StringBuffer getOutput() {
		return output;
	}
	public void setOutput(StringBuffer output) {
		this.output = output;
	}

	public HashMap<String, String> getFilterMappingGloria2ACP() {
		return filterMappingGloria2ACP;
	}

	public void setFilterMappingGloria2ACP(
			HashMap<String, String> filterMappingGloria2ACP) {
		this.filterMappingGloria2ACP = filterMappingGloria2ACP;
	}

	public String getProjectName() {
		return projectName;
	}

	public void setProjectName(String projectName) {
		this.projectName = projectName;
	}

	public String getRequestUser() {
		return requestUser;
	}

	public void setRequestUser(String requestUser) {
		this.requestUser = requestUser;
	}

	public String getRequestEmail() {
		return requestEmail;
	}

	public void setRequestEmail(String requestEmail) {
		this.requestEmail = requestEmail;
	}

	public String getRequestDesc() {
		return requestDesc;
	}

	public void setRequestDesc(String requestDesc) {
		this.requestDesc = requestDesc;
	}

	public ObservingPlan getOp() {
		return op;
	}

	public void setOp(ObservingPlan op) {
		this.op = op;
	}

	public Observer getObserver() {
		return observer;
	}

	public void setObserver(Observer observer) {
		this.observer = observer;
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

	public RtmlSkyCondition getSkyCondition() {
		return skyCondition;
	}

	public void setSkyCondition(RtmlSkyCondition skyCondition) {
		this.skyCondition = skyCondition;
	}
	

}
