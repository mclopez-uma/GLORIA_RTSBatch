package eu.gloria.rt.worker.offshore.acp;

import eu.gloria.rt.db.scheduler.ObservingPlanType;
import eu.gloria.rt.entity.db.FileContentType;

public class WorkerBiasRetrieverACP extends WorkerSystemOPRetriever{

	public WorkerBiasRetrieverACP() {
		super("Bias", ObservingPlanType.BIAS, eu.gloria.rt.entity.db.ObservingPlanType.BIAS, FileContentType.BIAS);
	}
	

}
