package eu.gloria.rt.worker.offshore.acp;

import eu.gloria.rt.db.scheduler.ObservingPlanType;
import eu.gloria.rt.entity.db.FileContentType;

public class WorkerDarkRetrieverACP extends WorkerSystemOPRetriever {

	public WorkerDarkRetrieverACP() {
		
		super("Dark", ObservingPlanType.DARK, eu.gloria.rt.entity.db.ObservingPlanType.DARK, FileContentType.DARK);
	}
	
	

}
