package eu.gloria.rt.worker.offshore.acp.web;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.mime.MultipartEntity;
import org.apache.http.entity.mime.content.StringBody;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;

public class DeletePlan {

	//public static final String StatusUrl = Constants.AcpSchedulerURL("seditplan.asp");

	/**
	 * ID del proyecto que deseamos eliminar.
	 */
	private int planID;

	private DefaultHttpClient httpclient;

	private HttpPost httppost;
	
	private String acpUrl;

	/**
	 * 
	 * @param projectID ID del proyecto a eliminar.
	 */
	public DeletePlan(int planID, String acpUrl, String host, int port, String user, String pw) {
		this.httpclient = new DefaultHttpClient();
		AuthScope authScope;
		authScope = new AuthScope(host, port);
		UsernamePasswordCredentials unamePwCredentials;
		unamePwCredentials = new UsernamePasswordCredentials(user, pw);
		this.httpclient.getCredentialsProvider().setCredentials(	authScope ,unamePwCredentials	);
		this.planID = planID;
		this.httppost = null;
		this.acpUrl = acpUrl;
	}

	private void generaPost ()
	{
		MultipartEntity reqEntity;

		this.httppost = new HttpPost(acpUrl);
		reqEntity = new MultipartEntity();
		StringBody parameterID_Value;
		StringBody parameterOperation_Value;

		parameterID_Value = null;

		try {
			parameterID_Value = new StringBody(""+this.planID);
		} catch (UnsupportedEncodingException e) {
			System.err.println("error al crear el parameterID_Value ("+this.planID+"):"+e.getMessage() );
		}
		reqEntity.addPart("id", parameterID_Value);

		parameterOperation_Value = null;
		try {
			parameterOperation_Value = new StringBody("del");
		} catch (UnsupportedEncodingException e) {
			System.err.println("error al crear el parameterOperation_Value (del):"+e.getMessage() );
		}
		reqEntity.addPart("op",parameterOperation_Value);

		this.httppost.setEntity(reqEntity);
	}

	/**
	 * Lee el contenido HTML de una URL
	 */
	public void readHtml()
	{
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
				sbLineas.append("\n");
				System.out.println(line);
			}

			if (resEntity != null) {
				System.out.println("Response content length: " + resEntity.getContentLength());
			}
			System.out.println("----------------------------------------");
			System.out.println("respuesta POST="+sbLineas.toString());
			EntityUtils.consume(resEntity);

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

	public void analizaHtml()
	{

	}

	public static void main(String[] args) throws Exception {
		/*DeletePlan deletePlan;
		deletePlan = new DeletePlan(5086);
		deletePlan.readHtml();
		deletePlan.analizaHtml();*/
		//System.out.println("scStatus.projectID="+scStatus.projectID);
	}
}