package eu.gloria.rt.worker.offshore.acp.web;

import java.util.Vector;

import eu.gloria.tools.log.LogUtil;

public class ACPManager {
	
	private String host;
	private int port;
	private String user;
	private String pw;
	private String urlBase;
	private int projectID;
	
	public ACPManager(int projectID, String urlBase, String host, int port, String user, String pw){
		this.host = host;
		this.port = port;
		this.user = user;
		this.pw = pw;
		this.urlBase = urlBase;
		this.projectID = projectID;
	}
	
	private String getAcpSchedulerURL(String aspPage)
	{
		return String.format("%s/%s", this.urlBase, aspPage);
	}
	
	public void removePlan(String opUuid) throws Exception {
	
		try{
			
			//Be sure a session is openned
			LogUtil.info(this, "ACPManager.removePlan:: opening session.");
			ACPSession acpSession = new ACPSession(getAcpSchedulerURL("index.asp"), host, port, user, pw);
			acpSession.open();
			
			//Start the main process...
			String acpPageUrlStatus = getAcpSchedulerURL("pstatus.asp");
			
			LogUtil.info(this, "ACPManager.removePlan:: OP UUID=" + opUuid);
			LogUtil.info(this, "ACPManager.removePlan:: Status URL=" + acpPageUrlStatus);
			
			boolean deleted = false;
			String opName = "G" + opUuid;
			
			//String user, String pw
			ProjectPlans projectPlans = new ProjectPlans(projectID, acpPageUrlStatus, host, port, user, pw);
			Vector<Plan> plans = projectPlans.getProjctPlans();
			
			LogUtil.info(this, "ACPManager.removePlan:: Looking for OP by OpName=" + opName);
			
			
			if (plans != null){
				for (Plan plan : plans) {
					if (plan.getName() != null && plan.getName().equals(opName)){ //Found
						
						LogUtil.info(this, "ACPManager.removePlan:: GLORIA project  OpList. Name=[" + plan.getName() + "]. FOUND!!!! -> remove");
						
						String acpPageUrlPlanDelete = getAcpSchedulerURL("seditplan.asp");
						
						LogUtil.info(this, "ACPManager.removePlan:: Deleting plan ID=" + plan.getId());
						LogUtil.info(this, "ACPManager.removePlan:: Delete URL=" + acpPageUrlPlanDelete);
						
						DeletePlan deletePlan = new DeletePlan(plan.getId(), acpPageUrlPlanDelete, host, port, user, pw);
						deletePlan.readHtml();
						deletePlan.analizaHtml();
						
						deleted = true;
						
						break;
						
					}
					
					LogUtil.info(this, "ACPManager.removePlan:: GLORIA project  OpList. Name=[" + plan.getName() + "]. It's not the right OP.");
				}
				
			}
			
			if (!deleted) LogUtil.severe(this, "ACPManager.removePlan:: The Plan does not exist in ACP. UUID=" + opName);
			
		}catch(Exception ex){
			ex.printStackTrace();
			throw ex;
		}
		
	}
	
	public void uploadRTML(String file) throws Exception {
		
		String acpPageUrl = getAcpSchedulerURL("suploadrtml.asp");
		
		RTMLUploader uploader = new RTMLUploader();
		uploader.upload(user, pw, host, String.valueOf(port), acpPageUrl, file);
		
	}

}
