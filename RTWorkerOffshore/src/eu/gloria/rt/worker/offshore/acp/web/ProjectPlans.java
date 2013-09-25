package eu.gloria.rt.worker.offshore.acp.web;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.io.UnsupportedEncodingException;
import java.util.Vector;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.mime.MultipartEntity;
import org.apache.http.entity.mime.content.StringBody;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

public class ProjectPlans {

	//public static final String StatusUrl = Constants.AcpSchedulerURL("pstatus.asp");

	private int projectID;
	private DefaultHttpClient httpclient;
	private Document xmlDoc;

	private HttpPost httppost;
	private String acpStatusUrl;

	/**
	 * 
	 * @param projectID ID del proyecto para el cual estamos buscando su lista de planes.
	 */
	public ProjectPlans(int projectID, String statusUrl, String host, int port, String user, String pw) {
		
		System.out.println("ProjectPlans.Constructor(). projectID=" +projectID );
		System.out.println("ProjectPlans.Constructor(). statusUrl=" +statusUrl );
		System.out.println("ProjectPlans.Constructor(). host=" +host );
		System.out.println("ProjectPlans.Constructor(). port=" +port );
		System.out.println("ProjectPlans.Constructor(). user=" +user );
		System.out.println("ProjectPlans.Constructor(). pw=" +pw );
		
		httpclient = new DefaultHttpClient();
		AuthScope authScope;
		authScope = new AuthScope(host, port);
		UsernamePasswordCredentials unamePwCredentials;
		unamePwCredentials = new UsernamePasswordCredentials(user, pw);
		httpclient.getCredentialsProvider().setCredentials(	authScope ,unamePwCredentials	);
		this.projectID = projectID;
		
		this.xmlDoc = null;
		this.acpStatusUrl = statusUrl;
	}
	
	public ProjectPlans() {
		

		this.xmlDoc = null;

	}

	private void generaPost()
	{
		MultipartEntity reqEntity;

		httppost = new HttpPost(acpStatusUrl + "?rid=" + this.projectID );
		reqEntity = new MultipartEntity();
		StringBody parameterID_Value;
		parameterID_Value = null;
		System.out.println("ProjectPlans. generatePost(). ProjectId=" +this.projectID );
		try {
			parameterID_Value = new StringBody(""+this.projectID);
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		reqEntity.addPart("rid", parameterID_Value);
		httppost.setEntity(reqEntity);
	}
	
	/**
	 * Lee el contenido HTML de una URL
	 */
	public void readHtml(){
		this.generaPost();
		HttpResponse response;
		HttpEntity resEntity;
		
		try {

			System.out.println("executing request:'" + httppost.getRequestLine()+"'");
			response = httpclient.execute(httppost);
			resEntity = response.getEntity();

			System.out.println("----------------------------------------");
			System.out.println("request status:'"+response.getStatusLine()+"'");

			BufferedReader rd;
			rd = new BufferedReader(new InputStreamReader(response.getEntity().getContent()));

			String line;// = "";
			StringBuilder sbLineas;
			sbLineas = new StringBuilder();
			while ((line = rd.readLine()) != null) {
				sbLineas.append(line);
				//sbLineas.append("\n");
System.out.println(line);
			}

			if (resEntity != null) {
				System.out.println("Response content length: " + resEntity.getContentLength());
			}
			System.out.println("----------------------------------------");

			EntityUtils.consume(resEntity);
			//////////////////////
			DocumentBuilderFactory docBuilderFactory;
			DocumentBuilder docBuilder;
			docBuilderFactory = DocumentBuilderFactory.newInstance();
			docBuilder = docBuilderFactory.newDocumentBuilder();

			Pattern patron;
			Matcher calce;
			patron = Pattern.compile("<table id=(.*?)</table>");
			calce = patron.matcher(sbLineas.toString());
			if (calce.find())
			{
				String strTable;
				strTable = calce.group(); 
				System.out.println("table="+strTable);
				this.xmlDoc = docBuilder.parse(new InputSource(new StringReader(strTable)));
			}
			//////////////////////

		}catch(Exception ex){
			ex.printStackTrace();
			System.out.println("Exception: message="+ex.getMessage());
		}
		finally {
			try {
				httpclient.getConnectionManager().shutdown();
			} catch (Exception ignore) {
				System.out.println("Exception: message="+ignore.getMessage());
			}
		}
	}
	
	/*public void readHtml(String file){
		
		try {


			BufferedReader rd;
			rd = new BufferedReader(new FileReader(file));

			String line;// = "";
			StringBuilder sbLineas;
			sbLineas = new StringBuilder();
			while ((line = rd.readLine()) != null) {
				sbLineas.append(line);
				//sbLineas.append("\n");
				System.out.println(line);
			}

			//////////////////////
			DocumentBuilderFactory docBuilderFactory;
			DocumentBuilder docBuilder;
			docBuilderFactory = DocumentBuilderFactory.newInstance();
			docBuilder = docBuilderFactory.newDocumentBuilder();

			Pattern patron;
			Matcher calce;
			patron = Pattern.compile("<table id=(.*?)</table>");
			calce = patron.matcher(sbLineas.toString());
			if (calce.find())
			{
				String strTable;
				strTable = calce.group(); 
				System.out.println("table="+strTable);
				this.xmlDoc = docBuilder.parse(new InputSource(new StringReader(strTable)));
			}
			//////////////////////

		}catch(Exception ex){
			ex.printStackTrace();
			System.out.println("Exception: message="+ex.getMessage());
		}
		finally {
			try {
				httpclient.getConnectionManager().shutdown();
			} catch (Exception ignore) {
				System.out.println("Exception: message="+ignore.getMessage());
			}
		}
	}*/


	/**
	 * analiza el HTML para llenar el vector de planes.
	 */
	public void analizaHtml(Vector<Plan> projectPlans)
	{

		NodeList listaRows;
		listaRows = this.xmlDoc.getElementsByTagName("tr");
		int cantRows;
		cantRows = listaRows.getLength();

		for (int i=0;i<cantRows;i++) {			
			Node nodo;
			nodo = listaRows.item(i);
			Plan planLocal;
			planLocal = new Plan(nodo);
			projectPlans.add(planLocal);
		}
	}

	
	public Vector<Plan> getProjctPlans() {
		
		Vector<Plan> projectPlans = new Vector<Plan>();
		
		/*readHtml("c:\\dummy\\OPlist.html");
		analizaHtml(projectPlans);*/
		
		readHtml();
		analizaHtml(projectPlans);
		
		return projectPlans;
	}

	/*public void setProjctPlans(Vector<Plan> projctPlans) {
		this.projctPlans = projctPlans;
	}*/

	public static void main(String[] args) throws Exception {
	/*	ProjectPlans scStatus;
		scStatus = new ProjectPlans();
		//scStatus.readHtml("c:\\dummy\\OPlist.html");
		for (Plan plan : scStatus.getProjctPlans()) {
			System.out.println("-----");
			System.out.println(plan.toString());
		}*/
		//System.out.println("scStatus.projectID="+scStatus.projectID);
	}
}
