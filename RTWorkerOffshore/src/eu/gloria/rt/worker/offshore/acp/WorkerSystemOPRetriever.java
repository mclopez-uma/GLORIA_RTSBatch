package eu.gloria.rt.worker.offshore.acp;

import java.io.File;
import java.nio.channels.ClosedByInterruptException;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

import javax.persistence.EntityManager;
import javax.xml.datatype.DatatypeFactory;
import javax.xml.datatype.XMLGregorianCalendar;

import eu.gloria.rt.catalogue.CatalogueTools;
import eu.gloria.rt.catalogue.Observer;
import eu.gloria.rt.catalogue.RTSInfo;
import eu.gloria.rt.db.scheduler.ObservingPlan;
import eu.gloria.rt.db.scheduler.ObservingPlanManager;
import eu.gloria.rt.db.scheduler.ObservingPlanState;
import eu.gloria.rt.db.scheduler.ObservingPlanType;
import eu.gloria.rt.db.util.DBUtil;
import eu.gloria.rt.entity.db.FileFormat;
import eu.gloria.rt.entity.db.FileType;
import eu.gloria.rt.entity.db.ObservingPlanOwner;
import eu.gloria.rt.entity.db.UuidType;
import eu.gloria.rt.exception.RTSchException;
import eu.gloria.rt.worker.core.Worker;
import eu.gloria.rti_db.tools.RTIDBProxyConnection;
import eu.gloria.tools.log.LogUtil;
import eu.gloria.tools.time.DateTools;

public class WorkerSystemOPRetriever extends Worker {
	
	protected String filePrefix;
	protected ObservingPlanType opDbType;
	protected eu.gloria.rt.entity.db.ObservingPlanType opRepType;
	protected eu.gloria.rt.entity.db.FileContentType fileRepContenType;
	
	public WorkerSystemOPRetriever( String filePrefix, ObservingPlanType opDbType,  eu.gloria.rt.entity.db.ObservingPlanType opRepType, eu.gloria.rt.entity.db.FileContentType fileRepContenType){
		
		super(); 
		
		this.filePrefix = filePrefix;
		this.opDbType = opDbType;
		this.opRepType = opRepType;
		this.fileRepContenType = fileRepContenType;
	}

	@Override
	protected void doAction() throws InterruptedException,
			ClosedByInterruptException, Exception {
		
		String imgsBasePath = getPropertyStringValue("imgsBasePath");
		
		String proxyDBHost = getPropertyStringValue("proxyDBHost");
		String proxyDBPort = getPropertyStringValue("proxyDBPort");
		String proxyDBAppName = getPropertyStringValue("proxyDBAppName");
		String proxyDBUser = getPropertyStringValue("proxyDBUser");
		String proxyDBPw = getPropertyStringValue("proxyDBPw");
		boolean proxyDBHttps = Boolean.parseBoolean(getPropertyStringValue("proxyDBHttps"));
		String proxyDBCertRep = getPropertyStringValue("proxyDBCertRep");
		
		Observer observer = new Observer();
		observer.setLatitude(getPropertyDoubleValue("obs_latitude"));
		observer.setLongitude(getPropertyDoubleValue("obs_longitude"));
		observer.setAltitude(getPropertyDoubleValue("obs_altitude"));
		
		EntityManager em = DBUtil.getEntityManager();
		ObservingPlanManager manager = new ObservingPlanManager();
		
		ObservingPlan dbOp = null;
		
		RTIDBProxyConnection dbProxy = null;
		String opUUID = null;
		
		try{
			
			DBUtil.beginTransaction(em);
			
			//Creates the db webservice proxy
			dbProxy = new RTIDBProxyConnection(proxyDBHost, proxyDBPort, proxyDBAppName, proxyDBUser, proxyDBPw, proxyDBHttps, proxyDBCertRep);
			
			File imgsPath = new File(imgsBasePath);
			if (!imgsPath.exists() || !imgsPath.isDirectory() ) throw new Exception("Local base directory for images does not exist.");
			
			for (File fileEntry : imgsPath.listFiles()) {
				
		        if (fileEntry.isDirectory()) {
		            LogUtil.severe(this, "There is a directory inside the images base directory!!!!!!. DIR=" + fileEntry.toString());
		        } else {
		        	
		        	if (fileEntry.getName().startsWith(filePrefix)){
		        		
		        		//Resolve the file format.
		            	FileFormat fileFormat = FileFormat.FITS;
		            	if (fileEntry.getName().endsWith("jpg")){
		            		fileFormat = FileFormat.JPG;
		            	} else if (fileEntry.getName().endsWith("fts")){
		            		fileFormat = FileFormat.FITS;
		            	} else{
		            		continue;
		            	}
		            	
		            	LogUtil.info(this, "FileFormat=" +fileFormat.toString());
		        		
		        		Date fileCreationDate = new Date(fileEntry.lastModified());
		        		Date observationSession = getObservationSessionDate(observer, fileCreationDate);
		        		
		        		LogUtil.info(this, "PROCESSING found file " + fileEntry.toString());
		        		
		        		dbOp = manager.get(em, observationSession, opDbType);
		        		
		        		if (dbOp == null){
		        			
		        			LogUtil.info(this, "Creating ObservingPlan.... ");
		        			
		        			//Creates the OP into DB
		        			opUUID = dbProxy.getProxy().uuidCreate(UuidType.OP);
		        			LogUtil.info(this, "Creating ObservingPlan. UUID=" + opUUID);
		        			dbOp = new ObservingPlan();
		        			dbOp.setUuid(opUUID);
		        			dbOp.setUser("SYSTEM");
		        			dbOp.setPriority(0);
		        			dbOp.setState(ObservingPlanState.DONE);
		        			dbOp.setType(opDbType);
		        			dbOp.setFile("none.xml");
		        			dbOp.setComment("Created by the system");
		        			dbOp.setExecDateObservationSession(observationSession);
		        			
		        			manager.create(em, dbOp);
		        			
		        		}
		        		
		        		LogUtil.info(this, "Recovering RepOP.... ");
		        		eu.gloria.rt.entity.db.ObservingPlan repOP = dbProxy.getProxy().opGet(dbOp.getUuid());
		        		if (repOP == null){
		        			LogUtil.info(this, "Creating RepOP.... ");
		        			//Creates the REPOSITORY OP
		        			repOP = new eu.gloria.rt.entity.db.ObservingPlan();
		        			repOP.setOwner(ObservingPlanOwner.SYSTEM);
		        			repOP.setType(opRepType);
		        			repOP.setUser("SYSTEM");
		        			repOP.setUuid(dbOp.getUuid());
		        			
		        			dbProxy.getProxy().opCreate(repOP);
		        		}
		        		
		        		
		            	//DBRepository->Create/recover the File information
		            	eu.gloria.rt.entity.db.File file = null;
		            	String repFileUUID = null;
		            	try{
	            			file = new eu.gloria.rt.entity.db.File();
	            			file.setContentType(fileRepContenType);
	            			file.setDate(getDate(new Date()));
	            			file.setType(FileType.IMAGE);
	            			repFileUUID = dbProxy.getProxy().fileCreate(dbOp.getUuid(), file);
	            			
		            		LogUtil.info(this, "CREATED GLORIA file UUID=" + repFileUUID);
	            			
	            		}catch(Exception ex){
							throw new Exception("Error registering a file into the DBRepository.");
						}
		            	
		            	//Creates the format
		            	String urlSource = "file://" + imgsBasePath + fileEntry.getName();
		            	LogUtil.info(this, "Source file url=" + urlSource);
		            	try{
	            			dbProxy.getProxy().fileAddFormat(repFileUUID, fileFormat, urlSource);
	            			
	            			LogUtil.info(this, "UPLOADED file format. url=" + urlSource);
	            		}catch(Exception ex){
							throw new Exception("Error adding a file format to a file into the DBRepository. urlSourcefile=" + urlSource);
						}
		            	
		        	}
		        	
		        }
		        
			}
			
			DBUtil.commit(em);
			
		}catch(Exception ex){
			
			ex.printStackTrace();
			
			try{
				if (dbProxy != null && opUUID != null) {
					dbProxy.getProxy().opDelete(opUUID);
				}
			}catch(Exception e){
				//Nothing
			}
			
			DBUtil.rollback(em);
			
			//To ensure the deletion
			try{
				if (dbOp != null && dbOp.getId() != null){
					manager.delete(null, dbOp.getId());
				}
			}catch(Exception e){
				e.printStackTrace();
			}
			
			
			throw new RTSchException(ex.getMessage());
			
		}finally{
			
			DBUtil.close(em);
			
		}
		
	}
	
	private Date getObservationSessionDate(Observer observer, Date date) throws Exception{
		RTSInfo info = CatalogueTools.getSunRTSInfo(observer, date);
		Date currentSession = DateTools.trunk(date, "yyyyMMdd");
		if (date.compareTo(info.getSet()) >= 0){ //after sun set -> currentSession + 1 day
			currentSession = DateTools.increment(currentSession, Calendar.DATE, 1);
		} else { //currentSession
			//Nothing
		}
		return currentSession;
	}
	
	private XMLGregorianCalendar getDate(Date date) throws Exception{
    	GregorianCalendar c = new GregorianCalendar();
		c.setTime(date);
		XMLGregorianCalendar xmlCalendar = DatatypeFactory.newInstance().newXMLGregorianCalendar(c);
		return xmlCalendar;
    }

}
