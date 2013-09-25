package eu.gloria.rt.worker.offshore.acp.rtml;


import java.text.ParseException;
import java.util.List;

import eu.gloria.rt.catalogue.Catalogue;
import eu.gloria.rt.catalogue.ObjCategory;
import eu.gloria.rt.catalogue.ObjInfo;
import eu.gloria.rt.exception.RTException;
import eu.gloria.rti.sch.core.ObservingPlan;
import eu.gloria.rti.sch.core.plan.constraint.Constraint;
import eu.gloria.rti.sch.core.plan.constraint.ConstraintMoonDistance;
import eu.gloria.rti.sch.core.plan.constraint.ConstraintTarget;
import eu.gloria.rti.sch.core.plan.constraint.ConstraintTargetAltitude;
import eu.gloria.rti.sch.core.plan.constraint.Constraints;
import eu.gloria.rti.sch.core.plan.instruction.CameraSettings;
import eu.gloria.rti.sch.core.plan.instruction.Expose;
import eu.gloria.rti.sch.core.plan.instruction.Instruction;
import eu.gloria.rti.sch.core.plan.instruction.Loop;
import eu.gloria.rti.sch.core.plan.instruction.Target;
import eu.gloria.tools.time.DateTools;


public class RtmlGenerator {
	
	private int targetCount;
	private int pictureCount;
	
	public String generateRtml(RtmlGeneratorContext context) throws RtmlException{
		
		this.targetCount = 1;
		this.pictureCount = 1;
		
		StringBuffer sb = new StringBuffer();
		String requestId = "G" + context.getRequestId();
		
		context.setOutput(sb);
		context.setRequestId(requestId);
		
		Constraints constraints = context.getOp().getConstraints();
		
		//Determines the description
		String tmpOPDescription = context.getRequestDesc();
		if (constraints != null && constraints.getTargets() != null ){
			List<Constraint> targets = constraints.getTargets();
			tmpOPDescription = tmpOPDescription + "TARGETS: ";
			for (Constraint constraint : targets) {
				ConstraintTarget tmp = (ConstraintTarget) constraint;
				if (tmp.getObjName() != null){
					tmpOPDescription = tmpOPDescription + " [" + tmp.getObjName() + "] ";
				}else{
					tmpOPDescription = tmpOPDescription + " [RA=" + tmp.getCoordinates().getJ2000().getRa() + ", DEC=" +  tmp.getCoordinates().getJ2000().getDec() + "] ";
				}
			}
		}
		context.setRequestDesc(tmpOPDescription);
		
		sb.append("<?xml version=\"1.0\" encoding=\"ISO8859-1\"?>").append("\n");
		sb.append("<RTML version=\"2.3\">").append("\n");
		sb.append("	<Contact>").append("\n");
		sb.append("		<User>").append(context.getRequestUser()).append("</User>").append("\n");
		sb.append("		<Email>").append(context.getRequestEmail()).append("</Email>").append("\n");
		sb.append("	</Contact>").append("\n");
		sb.append("	<Request>").append("\n");
		sb.append("		<ID>").append(requestId).append("</ID>").append("\n");
		sb.append("		<UserName>").append(context.getRequestUser()).append("</UserName>").append("\n");
		sb.append("		<Description>").append(context.getRequestDesc()).append("</Description>").append("\n");
		sb.append("		<Project>").append(context.getProjectName()).append("</Project>").append("\n");
		sb.append("		<Schedule>").append("\n");
		
		//------------CONSTRAINTS-------------
		if (constraints != null && constraints.getTargetAltitude() != null ){
			ConstraintTargetAltitude constTargetAltitude = (ConstraintTargetAltitude)constraints.getTargetAltitude();
			double targetAltitude = constTargetAltitude.getAltitude();
			if ( targetAltitude < 30) targetAltitude = 30;
			sb.append("			<Horizon>").append(targetAltitude).append("</Horizon>").append("\n");
		}else{
			sb.append("			<Horizon>").append(30).append("</Horizon>").append("\n");
		}
		
		/*sb.append("			<HourAngleRange>");
		sb.append("				<East>0</East>");
		sb.append("				<West>3</West>");
		sb.append("			</HourAngleRange>  ");*/
		
		if (context.getSkyCondition() != RtmlSkyCondition.None){
			sb.append("			<SkyCondition>").append(context.getSkyCondition().toString()).append("</SkyCondition>").append("\n");
		}
		
		
		if (constraints != null){
			
			//MOON
			sb.append("			<Moon>").append("\n");
			double moonDistance = 15;
			if (constraints.getMoonDistance()!= null){
				ConstraintMoonDistance constDistance = (ConstraintMoonDistance) constraints.getMoonDistance();
				if (constDistance.getDistance() >15) moonDistance = constDistance.getDistance();
			}
			sb.append("				<Distance>").append(moonDistance).append("</Distance>").append("\n");
			sb.append("				<Width>6</Width>\n");
			sb.append("			</Moon>").append("\n");
		}
		
		if (context.getScheduleDateIni() != null || context.getScheduleDateEnd() != null){
			
			sb.append("			<TimeRange>\n");
			
			if (context.getScheduleDateIni() != null){ //<Earliest>2013-03-21T20:43:17.01</Earliest> --><Earliest>yyyy-MM-dd'T'HH:mm:ss</Earliest>
				try {
					//sb.append("			<Earliest>").append(DateTools.getDate(context.getScheduleDateIni(), "yyyy-MM-dd'T'HH:mm:ss")).append("</Earliest>\n");
					sb.append("			<Earliest>").append(DateTools.getDate(DateTools.getGMT(context.getScheduleDateIni()), "yyyy-MM-dd'T'HH:mm:ss")).append("</Earliest>\n");
				} catch (ParseException e) {
					throw new RtmlException("Error formating ScheduleDateIni. " + e.getMessage());
				}
			}else{
				sb.append("			<!-- NO TimeRange Earliest -->\n");
			}
			
			if (context.getScheduleDateEnd() != null){ //<Latest>2013-03-21T20:43:17.01</Latest> --><Latest>yyyy-MM-dd'T'HH:mm:ss</Latest>
				try {
					sb.append("			<Latest>").append(DateTools.getDate(DateTools.getGMT(context.getScheduleDateEnd()), "yyyy-MM-dd'T'HH:mm:ss")).append("</Latest>\n");
				} catch (ParseException e) {
					throw new RtmlException("Error formating ScheduleDateEnd. " + e.getMessage());
				}
			}else{
				sb.append("			<!-- NO TimeRange Latest -->\n");
			}
			
			sb.append("			</TimeRange>\n");
			
		}else{
			sb.append("			<!-- NO TimeRange -->\n");
		}
		
		
		
		sb.append("			<Priority>100</Priority>").append("\n");
		//------------CONSTRAINTS-------------
		sb.append("		</Schedule>").append("\n");
		
		//TARGETS
		List<Instruction> insts = context.getOp().getInstructions();
		generateTargets(insts, context);
		
		sb.append("	</Request>").append("\n");
		sb.append("</RTML>");
		
		return sb.toString();
	}
	
	
	private void generateTargets(List<Instruction> insts, RtmlGeneratorContext context) throws RtmlException{
		
		
		
		if (insts != null){
			for (int x= 0; x < insts.size(); x++){
				Instruction inst = insts.get(x);
				if (inst instanceof Target){
					context.setTarget((Target)inst);
				}else if (inst instanceof CameraSettings){
					context.setCameraSettings((CameraSettings)inst);
				}else if (inst instanceof Expose){
					context.setExpose((Expose) inst);
					generateExpose(context);
				}else if (inst instanceof Loop){
					//IGNORE
				}
				
			}
		}
		
	}
	
	private void generateExpose(RtmlGeneratorContext context) throws RtmlException{
		
		try{
			
			
			Expose expose = context.getExpose();
			
			if (expose.getRepeatDuration() != null) throw new RtmlException("UNSUPPORTED: Exposition repetition based in time (repeatDuration)");
			
			StringBuffer sb = context.getOutput();
			
			sb.append("		<Target count=\"1\" interval=\"0\" tolerance=\"0\">").append("\n");
			//sb.append("			<Name>").append(context.getRequestId()).append("</Name>").append("\n");
			sb.append("			<Name>").append(context.getRequestId() + "-Target" + this.targetCount).append("</Name>").append("\n");
			this.targetCount++;
			
			if (context.getTarget() != null){
				
				if (context.getTarget().getCoordinates() != null && context.getTarget().getCoordinates().getJ2000()!= null){
					
					double ra = context.getTarget().getCoordinates().getJ2000().getRa();
					double dec = context.getTarget().getCoordinates().getJ2000().getDec();
					
					sb.append("			<Coordinates>").append("\n");
					sb.append("				<RightAscension>").append(ra).append("</RightAscension>").append("\n");
					sb.append("				<Declination>").append(dec).append("</Declination>").append("\n");
					sb.append("			</Coordinates>").append("\n");
					
				} else if (context.getTarget().getObjName() != null){
					
					//Resolve the object data into the catalogue
					Catalogue catalogue = new Catalogue(context.getObserver().getLongitude(), context.getObserver().getLatitude(), context.getObserver().getAltitude());
					ObjInfo objInfo  = catalogue.getObject(context.getTarget().getObjName(), ObjCategory.MajorPlanetAndMoon);
					if (objInfo == null){
						objInfo  = catalogue.getObject(context.getTarget().getObjName(), ObjCategory.OutsideSSystemObj);
						if (objInfo == null){
							objInfo  = catalogue.getObject(context.getTarget().getObjName(), ObjCategory.MinorPlanetOrAsteroid);
							if (objInfo == null) throw new Exception("Unknown Target: " + context.getTarget().getObjName());
						}
					}
					
					if (objInfo.getCategory() == ObjCategory.OutsideSSystemObj){ //By RADEC
						
						sb.append("			<Coordinates>").append("\n");
						sb.append("				<RightAscension>").append(objInfo.getPosition().getRaDecimal()).append("</RightAscension>").append("\n");
						sb.append("				<Declination>").append(objInfo.getPosition().getDecDecimal()).append("</Declination>").append("\n");
						sb.append("			</Coordinates>").append("\n");
						
					}else{ //By name
						sb.append("			<Planet>").append(translatePlanetName(context.getTarget().getObjName())).append("</Planet>").append("\n");
					}
				}
			}
			
			int exposeCount = 1;
			if (expose.getRepeatCount() != null){
				exposeCount = expose.getRepeatCount().intValue();
			}
			sb.append("			<Picture count=\"").append(exposeCount).append("\">").append("\n");
			sb.append("				<Name>P" + this.pictureCount + "</Name>").append("\n"); //Change Request by Eduardo
			sb.append("				<ExposureTime>").append(expose.getExpositionTime()).append("</ExposureTime>").append("\n");
			if (context.getCameraSettings() != null && context.getCameraSettings().getBinning() != null){
				sb.append("				<Binning>").append(context.getCameraSettings().getBinning().getBinX()).append("</Binning>").append("\n"); //The X value
			}
			
			generateFilter(context);
			
			sb.append("			</Picture>").append("\n");
			
			sb.append("		</Target>").append("\n");
			
			this.pictureCount++;
			
		}catch(Exception ex){
			throw new RtmlException(ex.getMessage());
		}
		
	}
	
	private String translatePlanetName(String name){
		
		if (name != null){
			String tmpName = name.toLowerCase().trim();
			if (tmpName.equals("saturn")) return "Saturn";
			if (tmpName.equals("pluto")) return "Pluto";
			if (tmpName.equals("mercury")) return "Mercury";
			if (tmpName.equals("venus")) return "Venus";
			if (tmpName.equals("mars")) return "Mars";
			if (tmpName.equals("jupiter")) return "Jupiter";
			if (tmpName.equals("uranus")) return "Uranus";
			if (tmpName.equals("neptune")) return "Neptune";
			if (tmpName.equals("sun")) return "Sun";
			if (tmpName.equals("moon")) return "Moon";
			
			return name;
		}
		
		return null;
	}
	
	
	private void generateFilter(RtmlGeneratorContext context) throws RtmlException{
		
		String localFilter = null;
		
		if (context.getExpose().getFilter() != null){
			localFilter = context.getFilterMappingGloria2ACP().get(context.getExpose().getFilter());
		}else{
			localFilter = "Clear"; //By default
		}
		
		if (localFilter != null){
			StringBuffer sb = context.getOutput();
			sb.append("				<Filter>").append(localFilter).append("</Filter>").append("\n");
		}
		
	}
	
}
