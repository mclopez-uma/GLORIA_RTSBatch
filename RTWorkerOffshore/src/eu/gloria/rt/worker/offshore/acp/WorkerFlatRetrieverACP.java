package eu.gloria.rt.worker.offshore.acp;

import eu.gloria.rt.db.scheduler.ObservingPlanType;
import eu.gloria.rt.entity.db.FileContentType;

public class WorkerFlatRetrieverACP extends WorkerSystemOPRetriever {

	public WorkerFlatRetrieverACP() {
		super("AutoFlat", ObservingPlanType.FLAT, eu.gloria.rt.entity.db.ObservingPlanType.FLAT, FileContentType.FLAT);
	}

}
