package eu.gloria.rt.worker.offshore.acp;

import java.io.File;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.List;

import javax.persistence.EntityManager;
import javax.xml.datatype.DatatypeFactory;
import javax.xml.datatype.XMLGregorianCalendar;

import eu.gloria.rt.catalogue.CatalogueTools;
import eu.gloria.rt.catalogue.Observer;
import eu.gloria.rt.catalogue.RTSInfo;
import eu.gloria.rt.db.scheduler.ObservingPlan;
import eu.gloria.rt.db.scheduler.ObservingPlanManager;
import eu.gloria.rt.db.scheduler.ObservingPlanState;
import eu.gloria.rt.db.util.DBUtil;
import eu.gloria.rt.entity.db.FileContentType;
import eu.gloria.rt.entity.db.FileFormat;
import eu.gloria.rt.entity.db.FileType;
import eu.gloria.rt.entity.db.ObservingPlanOwner;
import eu.gloria.rt.entity.db.ObservingPlanType;
import eu.gloria.rt.exception.RTException;
import eu.gloria.rt.exception.RTSchException;
import eu.gloria.rt.tools.img.ConverterNetpbm;
import eu.gloria.rt.worker.offshore.acp.rtml.RtmlGenerator;
import eu.gloria.rt.worker.offshore.acp.rtml.RtmlGeneratorContext;
import eu.gloria.rt.worker.offshore.acp.web.ACPManager;
import eu.gloria.rt.worker.offshore.acp.web.RTMLUploader;
import eu.gloria.rti.sch.core.OffshorePluginRetriever;
import eu.gloria.rti.sch.core.OffshoreRetriever;
import eu.gloria.rti_db.tools.RTIDBProxyConnection;
import eu.gloria.tools.file.FileUtil;
import eu.gloria.tools.log.LogUtil;
import eu.gloria.tools.time.DateTools;

public class OffshoreRetrieverACP extends OffshorePluginRetriever implements OffshoreRetriever {
	
	@Override
	public void retrieve(String idOp) throws RTSchException {
		
		String acpImgsBasePath = getPropertyValueString("acpImgsBasePath");
		String acp_http_base_url = getPropertyValueString("acp_http_base_url");
		String acp_http_auth_user = getPropertyValueString("acp_http_auth_user");
		String acp_http_auth_pw = getPropertyValueString("acp_http_auth_pw");
		String acp_http_ip = getPropertyValueString("acp_http_ip");
		int acp_http_port = getPropertyValueInt("acp_http_port");
		int acp_gloria_project_id = getPropertyValueInt("acp_gloria_project_id"); 
		String proxyHost = getPropertyValueString("proxyHost");
		String proxyPort = getPropertyValueString("proxyPort");
		String proxyAppName = getPropertyValueString("proxyAppName");
		String proxyUser = getPropertyValueString("proxyUser");
		String proxyPw = getPropertyValueString("proxyPw");
		boolean proxyHttps = Boolean.parseBoolean(getPropertyValueString("proxyHttps"));
		String proxyCertRep = getPropertyValueString("proxyCertRep");
		
		Observer observer = new Observer();
		observer.setLatitude(getPropertyValueDouble("obs_latitude"));
		observer.setLongitude(getPropertyValueDouble("obs_longitude"));
		observer.setAltitude(getPropertyValueDouble("obs_altitude"));
		
		
		LogUtil.info(this, "OffshoreRetrieverACP.retrieve(" + idOp + "). BEGIN");
		
		EntityManager em = DBUtil.getEntityManager();
		ObservingPlanManager manager = new ObservingPlanManager();
		
		ObservingPlan dbOp = null;
		
		Date creationFileDateEarliest = null;
		Date creationFileDateLatest = null;
		
		try{
			
			DBUtil.beginTransaction(em);
			
			dbOp = manager.get(em, idOp);
			
			if (dbOp != null){
				
				try{
					
					File acpImgsPath = new File(acpImgsBasePath);
					if (!acpImgsPath.exists() || !acpImgsPath.isDirectory() ) throw new Exception("Local ACP base directory for images does not exist.");
					
					LogUtil.info(this, "OffshoreRetrieverACP.retrieve(" + idOp + "). acpImgPath exists: " + acpImgsBasePath);
					
					//Creates the db webservice proxy
					RTIDBProxyConnection dbProxy = new RTIDBProxyConnection(proxyHost, proxyPort, proxyAppName, proxyUser, proxyPw, proxyHttps, proxyCertRep);
					
					//DBRepository->Create the Observing Plan
					eu.gloria.rt.entity.db.ObservingPlan repOP = new eu.gloria.rt.entity.db.ObservingPlan();
					repOP.setOwner(ObservingPlanOwner.USER);
					repOP.setType(ObservingPlanType.OBSERVATION);
					repOP.setUser(dbOp.getUser());
					repOP.setUuid(dbOp.getUuid());
					
					try{
						String uuid = dbProxy.getProxy().opCreate(repOP);
						repOP = dbProxy.getProxy().opGet(uuid);
						
						LogUtil.info(this, "OffshoreRetrieverACP.retrieve(" + idOp + "). DBRepository OP created. UUID= " + uuid);
						
					}catch(Exception ex){
						throw new Exception("Error registering the Observing Plan into the DBRepository.");
					}
					
					
					String prefix = "G" + dbOp.getUuid();
					
					HashMap<String, String> fileMappingUUID = new HashMap<String, String>(); //Mapping between ACP file uuids and GLORIA UUIDS 
					List<File> uploadedFiles = new ArrayList<File>();
					
					for (File fileEntry : acpImgsPath.listFiles()) {
						
				        if (fileEntry.isDirectory()) {
				            LogUtil.severe(this, "There is a directory inside the ACP base directory!!!!!!. DIR=" + fileEntry.toString());
				        } else {
				            if (fileEntry.getName().startsWith(prefix)){ //Belongs to this ObservingPlan....
				            	
				            	//Resolve the file format.
				            	FileFormat fileFormat = FileFormat.FITS;
				            	if (fileEntry.getName().endsWith("jpg")){
				            		fileFormat = FileFormat.JPG;
				            	}
				            	
				            	if (fileFormat == FileFormat.JPG){ //The jpg file is removed
				            		fileEntry.delete();
				            		continue; //Next file....
				            	}
				            	
				            	LogUtil.info(this, "OffshoreRetrieverACP.retrieve(" + idOp + "). FileFormat=" +fileFormat.toString());		
				            	
				            	//Dates
				            	Date fileCreationDate = new Date(fileEntry.lastModified());
				            	if (creationFileDateEarliest == null || creationFileDateEarliest.compareTo(fileCreationDate) > 0) creationFileDateEarliest = fileCreationDate;
				            	if (creationFileDateLatest == null || creationFileDateLatest.compareTo(fileCreationDate) < 0) creationFileDateLatest = fileCreationDate;
				            	
				            	LogUtil.info(this, "OffshoreRetrieverACP.retrieve(" + idOp + "). PROCESSING found file " + fileEntry.toString());
				            	
				            	//DBRepository->Create/recover the File information
				            	String acpUUID = FileUtil.fileNameWithoutExtension(fileEntry.getName());
				            	LogUtil.info(this, "OffshoreRetrieverACP.retrieve(" + idOp + "). ACP UUID=" + acpUUID);
				            	String gloriaFileUUID = fileMappingUUID.get(acpUUID);
				            	eu.gloria.rt.entity.db.File file = null;
				            	if (gloriaFileUUID == null){ //Creates the Repository file.
				            		
				            		try{
				            			file = new eu.gloria.rt.entity.db.File();
				            			file.setContentType(FileContentType.OBSERVATION);
				            			file.setDate(getDate(new Date()));
				            			file.setType(FileType.IMAGE);
				            			gloriaFileUUID = dbProxy.getProxy().fileCreate(dbOp.getUuid(), file);
				            			
					            		LogUtil.info(this, "OffshoreRetrieverACP.retrieve(" + idOp + "). CREATED GLORIA file UUID=" + gloriaFileUUID);
				            			
				            			fileMappingUUID.put(acpUUID, gloriaFileUUID);
				            			
				            		}catch(Exception ex){
										throw new Exception("Error registering a file into the DBRepository.");
									}
				            		
				            	}else{
				            		
				            		try{
				            			LogUtil.info(this, "OffshoreRetrieverACP.retrieve(" + idOp + "). REUSED GLORIA file UUID=" + gloriaFileUUID);
				            			file = dbProxy.getProxy().fileGet(gloriaFileUUID);
				            		}catch(Exception ex){
										throw new Exception("Error recovering a file data from the DBRepository.");
									}
				            		
				            	}
				            	
				            	//Generate && upload && remove the format JPG....
				            	String filepnm = acpImgsBasePath + acpUUID + ".pnm";
				            	String filejpg = acpImgsBasePath + acpUUID + ".jpg";
				            	File pnm = new File(filepnm);
				        		File jpg = new File(filejpg);
				        		
				        		String urljpg = "file://" + filejpg;
				        		
				            	try{
				            		//jpg generation
				            		ConverterNetpbm converter = new ConverterNetpbm();
				            		converter.fitstopnm(fileEntry, pnm); //fits->pnm
				            		converter.pnmtojpeg(pnm, jpg);       //pnm->jpg
				            		
				            		//upload format
				            		LogUtil.info(this, "OffshoreRetrieverACP.retrieve(" + idOp + "). source file url=" + urljpg);
			            			dbProxy.getProxy().fileAddFormat(gloriaFileUUID, FileFormat.JPG, urljpg);
			            			uploadedFiles.add(jpg);
				            			
			            			LogUtil.info(this, "OffshoreRetrieverACP.retrieve(" + idOp + "). UPLOADED file format. url=" + urljpg);
			            			
				            	}catch(Exception ex){
				            		
				            		throw new Exception("Error adding a file format to a file into the DBRepository. urlSourcefile=[" + urljpg + "]. " + ex.getMessage());
				            		
				            	}finally{
				            		
				            		if (pnm.exists()) pnm.delete();
				            		if (jpg.exists()) jpg.delete();
				            		
				            	}

				            	//Creates the format FITS
				            	String urlSource = "file://" + acpImgsBasePath + fileEntry.getName();
				            	LogUtil.info(this, "OffshoreRetrieverACP.retrieve(" + idOp + "). source file url=" + urlSource);
				            	try{
			            			dbProxy.getProxy().fileAddFormat(gloriaFileUUID, fileFormat, urlSource);
			            			uploadedFiles.add(fileEntry);
			            			
			            			LogUtil.info(this, "OffshoreRetrieverACP.retrieve(" + idOp + "). UPLOADED file format. url=" + urlSource);
			            		}catch(Exception ex){
									throw new Exception("Error adding a file format to a file into the DBRepository. urlSourcefile=" + urlSource);
								}
				            	
				            }else{
				            	LogUtil.info(this, "OffshoreRetrieverACP.retrieve(" + idOp + "). IGNORED found file " + fileEntry.toString());
				            }
				        }
				    } //for
					
					//TODO: remove all files from acp directory
					
					
					if (uploadedFiles.size() == 0){
						
						dbOp.setState(ObservingPlanState.ABORTED);
						dbOp.setComment("No images from local executor (ACP).");
						
					}else{
						
						dbOp.setState(ObservingPlanState.DONE);
						
						if (creationFileDateEarliest != null) dbOp.setExecDateIni(creationFileDateEarliest);
						if (creationFileDateLatest != null) dbOp.setExecDateEnd(creationFileDateLatest);
						
						//Resolves the observation session.
						Date currentSession = getObservationSessionDate(observer, creationFileDateEarliest);
						dbOp.setExecDateObservationSession(currentSession);
					}
					
				}catch(Exception ex){
					
					ex.printStackTrace();
					
					dbOp.setState(ObservingPlanState.ERROR);
					dbOp.setComment("ERROR: " + ex.getMessage());
				}
				
				//Delete OP from ACP.
				
				ACPManager acpManager = new ACPManager(acp_gloria_project_id, acp_http_base_url, acp_http_ip, acp_http_port, acp_http_auth_user, acp_http_auth_pw);
				acpManager.removePlan(dbOp.getUuid());
				
			}else{
				
				throw new Exception("OffshoreRetrieverACP. The observing plan does not exist. ID=" + idOp);
				
			}
			
			DBUtil.commit(em);
			
		} catch (Exception ex) {
			
			ex.printStackTrace();
			
			DBUtil.rollback(em);
			throw new RTSchException(ex.getMessage());
			
		} finally {
			DBUtil.close(em);
		}
		
		LogUtil.info(this, "OffshoreRetrieverACP.retrieve(" + idOp + "). END");
		
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
