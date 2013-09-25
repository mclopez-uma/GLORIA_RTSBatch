package eu.gloria.rt.worker.offshore.talon;

import java.io.File;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
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
import eu.gloria.rt.exception.RTSchException;
import eu.gloria.rt.tools.img.ConverterNetpbm;
import eu.gloria.rt.worker.offshore.talon.telrun.TelrunDB;
import eu.gloria.rt.worker.offshore.talon.telrun.TelrunEntry;
import eu.gloria.rti.sch.core.OffshorePluginRetriever;
import eu.gloria.rti.sch.core.OffshoreRetriever;
import eu.gloria.rti_db.tools.RTIDBProxyConnection;
import eu.gloria.tools.log.LogUtil;
import eu.gloria.tools.time.DateTools;

public class OffshoreRetrieverTALON extends OffshorePluginRetriever implements OffshoreRetriever  {
	
	@Override
	public void retrieve(String idOp) throws RTSchException {
		
		String proxyHost = getPropertyValueString("proxyHost");
		String proxyPort = getPropertyValueString("proxyPort");
		String proxyAppName = getPropertyValueString("proxyAppName");
		String proxyUser = getPropertyValueString("proxyUser");
		String proxyPw = getPropertyValueString("proxyPw");
		boolean proxyHttps = Boolean.parseBoolean(getPropertyValueString("proxyHttps"));
		String proxyCertRep = getPropertyValueString("proxyCertRep");
		
		String telrunFilePath = getPropertyValueString("telrun.sls_fullpath");
		String talonImgsPath = getPropertyValueString("talonImgsPath");
		
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
		TelrunDB telrunDB = null;
		
		List<File> uploadedFiles = new ArrayList<File>();
		
		try{
			
			DBUtil.beginTransaction(em);
			
			dbOp = manager.get(em, idOp);
			
			if (dbOp != null){
				
				try{
					
					//PREFIX= GLYYMMDD
					String filePrefix = getFilePrefix(observer, dbOp.getScheduleDateIni());
					
					File telrunFile = new File(telrunFilePath + filePrefix + ".sls");
					if (!telrunFile.exists()) {
						LogUtil.severe(this, "OffshoreRetrieverTALON.retrieve(" + idOp + "). telrun.sls file does not exist: " + telrunFilePath);
						//throw new Exception("The OP is not executed by the local executor (TALON). [telrun.sls file does not exist]");
					}else{
						LogUtil.info(this, "OffshoreRetrieverTALON.retrieve(" + idOp + "). telrun.sls file exists: " + telrunFilePath);
					}
					
					try{
						telrunDB = new TelrunDB(telrunFile);
					}catch(Exception ex){
						ex.printStackTrace();
						LogUtil.severe(this, "OffshoreRetrieverTALON.retrieve(" + idOp + "). telrun.sls cannot be read: " + ex.getMessage());
						//throw new Exception("The OP is not executed by the local executor (TALON). [telrun.sls cannot be read]");
					}
					
					//OP executed?
					if (telrunDB.isOpExecuted(dbOp.getUuid())){
						
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
							
							LogUtil.info(this, "OffshoreRetrieverTALON.retrieve(" + idOp + "). DBRepository OP created. UUID= " + uuid);
							
						}catch(Exception ex){
							throw new Exception("Error registering the Observing Plan into the DBRepository.");
						}
						
						List<TelrunEntry> entries =  telrunDB.getEntries(dbOp.getUuid());
						
						for (int x = 0; x < entries.size(); x++){
							
							TelrunEntry entry = entries.get(x);
							if (entry.getImage() == null || entry.getImage().trim().isEmpty()){
								continue; //Look for next entry if no image is available
							}
							
							File fileEntry = new File(entry.getImage());
							
							//Resolve the file format.
			            	FileFormat fileFormat = FileFormat.FITS;
			            	if (fileEntry.getName().endsWith("jpg")){
			            		fileFormat = FileFormat.JPG;
			            	}
			            	
			            	if (fileFormat == FileFormat.JPG){ //The jpg file is removed
			            		fileEntry.delete();
			            		continue; //Next file....
			            	}
			            	
			            	LogUtil.info(this, "OffshoreRetrieverTALON.retrieve(" + idOp + "). FileFormat=" +fileFormat.toString());
			            	
			            	//Dates
			            	Date fileCreationDate = new Date(fileEntry.lastModified());
			            	if (creationFileDateEarliest == null || creationFileDateEarliest.compareTo(fileCreationDate) > 0) creationFileDateEarliest = fileCreationDate;
			            	if (creationFileDateLatest == null || creationFileDateLatest.compareTo(fileCreationDate) < 0) creationFileDateLatest = fileCreationDate;
			            	
			            	LogUtil.info(this, "OffshoreRetrieverTALON.retrieve(" + idOp + "). PROCESSING found file " + fileEntry.toString());
			            	
			            	//DBRepository->Create/recover the File information
			            	
			            	eu.gloria.rt.entity.db.File file = null;
			            	String gloriaFileUUID = null;
			            	try{
			            		file = new eu.gloria.rt.entity.db.File();
			            		file.setContentType(FileContentType.OBSERVATION);
			            		file.setDate(getDate(new Date()));
			            		file.setType(FileType.IMAGE);
			            		gloriaFileUUID = dbProxy.getProxy().fileCreate(dbOp.getUuid(), file);
			            			
				            	LogUtil.info(this, "OffshoreRetrieverTALON.retrieve(" + idOp + "). CREATED GLORIA file UUID=" + gloriaFileUUID);
			            			
			            	}catch(Exception ex){
			            		ex.printStackTrace();
								throw new Exception("Error registering a file into the DBRepository:" + ex.getMessage());
			            	}
			            	
			            	//Generate && upload && remove the format JPG....
			            	/*String filepnm = talonImgsPath + dbOp.getUuid() + ".pnm";
			            	String filejpg = talonImgsPath + dbOp.getUuid() + ".jpg";
			            	File pnm = new File(filepnm);
			        		File jpg = new File(filejpg);
			        		
			        		String urljpg = "file://" + filejpg;
			        		
			            	try{
			            		//jpg generation
			            		ConverterNetpbm converter = new ConverterNetpbm();
			            		converter.fitstopnm(fileEntry, pnm); //fits->pnm
			            		converter.pnmtojpeg(pnm, jpg);       //pnm->jpg
			            		
			            		//upload format
			            		LogUtil.info(this, "OffshoreRetrieverTALON.retrieve(" + idOp + "). source file url=" + urljpg);
		            			dbProxy.getProxy().fileAddFormat(gloriaFileUUID, FileFormat.JPG, urljpg);
		            			uploadedFiles.add(jpg);
			            			
		            			LogUtil.info(this, "OffshoreRetrieverTALON.retrieve(" + idOp + "). UPLOADED file format. url=" + urljpg);
		            			
			            	}catch(Exception ex){
			            		
			            		throw new Exception("Error adding a file format to a file into the DBRepository. urlSourcefile=[" + urljpg + "]. " + ex.getMessage());
			            		
			            	}finally{
			            		
			            		if (pnm.exists()) pnm.delete();
			            		if (jpg.exists()) jpg.delete();
			            		
			            	}*/
			            	
			            	//Creates the format FITS
			            	String urlSource = "file://" + entry.getImage();
			            	LogUtil.info(this, "OffshoreRetrieverTALON.retrieve(" + idOp + "). source file url=" + urlSource);
			            	try{
			            		
		            			dbProxy.getProxy().fileAddFormat(gloriaFileUUID, fileFormat, urlSource);
		            			uploadedFiles.add(fileEntry);
		            			
		            			LogUtil.info(this, "OffshoreRetrieverTALON.retrieve(" + idOp + "). UPLOADED file format. url=" + urlSource);
		            			
		            		}catch(Exception ex){
								throw new Exception("Error adding a file format to a file into the DBRepository. urlSourcefile=" + urlSource);
							}
			            		
						}
						
					}
					
					if (uploadedFiles.size() == 0){
						
						dbOp.setState(ObservingPlanState.ABORTED);
						dbOp.setComment("No images from local executor (TALON).");
						
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
				
			}else{
				
				throw new Exception("OffshoreRetrieverTALON. The observing plan does not exist. ID=" + idOp);
				
			}
			
			DBUtil.commit(em);
			
		} catch (Exception ex) {
			
			ex.printStackTrace();
			
			DBUtil.rollback(em);
			throw new RTSchException(ex.getMessage());
			
		} finally {
			
			DBUtil.close(em);
			
			if (telrunDB != null) {
				telrunDB.clear();
			}
		}
		
		LogUtil.info(this, "OffshoreRetrieverTALON.retrieve(" + idOp + "). END");
		
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
	
	private String getFilePrefix(Observer observer, Date schIniDate) throws Exception{
		RTSInfo info = CatalogueTools.getSunRTSInfo(observer, schIniDate);
		
		LogUtil.info(this, "OffshoreRetrieverTALON.Calculating SCH file prefix. schIniDate=" + schIniDate);
		if (schIniDate.compareTo(info.getSet()) >= 0){ //after sun set -> schIniDate
			//Nothing
			LogUtil.info(this, "OffshoreRetrieverTALON. schIniDate after sunset=" + info.getSet());
		} else {  //before sun set -> prefixDate = schInitDate - 1
			schIniDate = DateTools.increment(schIniDate, Calendar.DATE, -1);
			LogUtil.info(this, "OffshoreRetrieverTALON. schIniDate before sunset=" + info.getSet());
		}
		LogUtil.info(this, "OffshoreRetrieverTALON.Calculated SCH file prefix. schIniDate=" + schIniDate);
		return  "GL" + DateTools.getDate(schIniDate, "yyMMdd");
	}
	
	private XMLGregorianCalendar getDate(Date date) throws Exception{
    	GregorianCalendar c = new GregorianCalendar();
		c.setTime(date);
		XMLGregorianCalendar xmlCalendar = DatatypeFactory.newInstance().newXMLGregorianCalendar(c);
		return xmlCalendar;
    }

}
