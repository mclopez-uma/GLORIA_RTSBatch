package eu.gloria.rt.worker.offshore.talon;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import javax.persistence.EntityManager;

import eu.gloria.rt.catalogue.CatalogueTools;
import eu.gloria.rt.catalogue.Observer;
import eu.gloria.rt.catalogue.RTSInfo;
import eu.gloria.rt.db.scheduler.ObservingPlan;
import eu.gloria.rt.db.scheduler.ObservingPlanManager;
import eu.gloria.rt.db.scheduler.ObservingPlanState;
import eu.gloria.rt.db.util.DBUtil;
import eu.gloria.rt.exception.RTSchException;
import eu.gloria.rt.worker.offshore.talon.sch.SchGenerator;
import eu.gloria.rt.worker.offshore.talon.sch.SchGeneratorContext;
import eu.gloria.rti.sch.core.OffshorePluginPublisher;
import eu.gloria.rti.sch.core.OffshorePublisher;
import eu.gloria.tools.log.LogUtil;
import eu.gloria.tools.time.DateTools;

public class OffshorePublisherHumanTALON extends OffshorePluginPublisher implements OffshorePublisher{
	
	@Override
	public void publish(String idOp) throws RTSchException {
		
		String xmlPath= getPropertyValueString("xmlPath");
		String schPath= getPropertyValueString("schPath");
		String opXSD = getPropertyValueString("opXSD");
		String filterMapping = getPropertyValueString("filterMapping");
		
		double obs_altitude = getPropertyValueDouble("obs_altitude");
		double obs_latitude = getPropertyValueDouble("obs_latitude");
		double obs_longitude = getPropertyValueDouble("obs_longitude");
		
		String talonImgsPath = getPropertyValueString("talonImgsPath");
		
		Observer observer = new Observer();
		observer.setAltitude(obs_altitude);
		observer.setLatitude(obs_latitude);
		observer.setLongitude(obs_longitude);
		
		
		EntityManager em = DBUtil.getEntityManager();
		ObservingPlanManager manager = new ObservingPlanManager();
		
		ObservingPlan dbOp = null;
		
		try{
			
			DBUtil.beginTransaction(em);
			
			dbOp = manager.get(em, idOp);
			
			if (dbOp != null){
				
				try{
					
					//Genenerate the folder for current ObservationSession
					/*String subFolder = DateTools.getDate(getObservationSessionDate(observer, dbOp.getScheduleDateIni()), "yyyyMMdd");
					
					schPath = schPath +  subFolder + File.separator;
					File outputSchPath = new File(schPath);
					if (!outputSchPath.exists()){
						if (!outputSchPath.mkdirs()){
							throw new Exception("Offshore::Impossible to create the output directory for SCH files. Directory=" + outputSchPath.getAbsolutePath());
						}
					}*/
					
					//PREFIX= GLYYMMDD
					String filePrefix = getFilePrefix(observer, dbOp.getScheduleDateIni());
					
					eu.gloria.rti.sch.core.ObservingPlan op = new eu.gloria.rti.sch.core.ObservingPlan(xmlPath + dbOp.getFile() , opXSD);
					
					SchGeneratorContext context = new SchGeneratorContext();
					context.setOp(op);
					context.loadFilterMapping(filterMapping);
					context.setObserver(observer);
					context.setScheduleDateIni(dbOp.getScheduleDateIni());
					context.setScheduleDateEnd(dbOp.getExecDeadline());
					context.setTalonImgsPath(talonImgsPath.substring(0, talonImgsPath.length() - 1));
					
					SchGenerator generator = new SchGenerator();
					generator.generateSchList(context);
					
					List<String> schList = context.getOutput();
					if (schList != null){
						for (int x = 0 ; x < schList.size(); x++){
							String fileName = filePrefix + "_" + dbOp.getUuid() + "_" + x +  ".sch";
							String schFileName = schPath + fileName;
							saveSchFile(schList.get(x), schFileName);
						}
					}
					
					dbOp.setEventOffshoreReqDate(new Date());
					dbOp.setState(ObservingPlanState.OFFSHORE);
				
				}catch(Exception ex){
					
					ex.printStackTrace();
					
					dbOp.setState(ObservingPlanState.ERROR);
					dbOp.setComment("ERROR: " + ex.getMessage());
				}
				
			}else{
				
				throw new Exception("OffshorePublisherTALON. The observing plan does not exist. ID=" + idOp);
				
			}
			
			DBUtil.commit(em);
			
		} catch (Exception ex) {
			
			DBUtil.rollback(em);
			throw new RTSchException(ex.getMessage());
			
		} finally {
			DBUtil.close(em);
		}
	}
	
	
	private void saveSchFile(String schContent, String schFileName) throws IOException{
		
	    File file = new File(schFileName);
	     
	    if (file.exists()) {
	    	file.delete();
	    }
	    
		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file), "8859_1"));
		
		try{
			bw.write(schContent);
		}finally{
			bw.close();
		}
		
	}
	
	private Date getObservationSessionDate(Observer observer, Date date) throws Exception{
		RTSInfo info = CatalogueTools.getSunRTSInfo(observer, date);
		Date currentSession = DateTools.trunk(date, "yyMMdd");
		if (date.compareTo(info.getSet()) >= 0){ //after sun set -> currentSession + 1 day
			currentSession = DateTools.increment(currentSession, Calendar.DATE, 1);
		} else { //currentSession
			//Nothing
		}
		return currentSession;
	}
	
	private String getFilePrefix(Observer observer, Date schIniDate) throws Exception{
		RTSInfo info = CatalogueTools.getSunRTSInfo(observer, schIniDate);
		
		LogUtil.info(this, "OffshorePublisherHumanTALON.Calculating SCH file prefix. schIniDate=" + schIniDate);
		if (schIniDate.compareTo(info.getSet()) >= 0){ //after sun set -> schIniDate
			//Nothing
			LogUtil.info(this, "OffshorePublisherHumanTALON. schIniDate after sunset=" + info.getSet());
		} else {  //before sun set -> prefixDate = schInitDate - 1
			schIniDate = DateTools.increment(schIniDate, Calendar.DATE, -1);
			LogUtil.info(this, "OffshorePublisherHumanTALON. schIniDate before sunset=" + info.getSet());
		}
		LogUtil.info(this, "OffshorePublisherHumanTALON.Calculated SCH file prefix. schIniDate=" + schIniDate);
		return  "GL" + DateTools.getDate(schIniDate, "yyMMdd");
	}
	
	

}
