package eu.gloria.rt.worker.offshore.acp.web;

import java.io.File;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.mime.MultipartEntity;
import org.apache.http.entity.mime.content.FileBody;
import org.apache.http.entity.mime.content.StringBody;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;

public class RTMLUploader {
	
	/*public static void main(String[] args) throws Exception {
		
		RTMLUploader uploader = new RTMLUploader();
		uploader.upload("/mnt/default/worker/dummy.rtml");
	}*/

	public void upload(String acp_http_auth_user, String acp_http_auth_pw, String acp_http_ip, String acp_http_port, String acp_http_rtml_upload_url,  String file) throws Exception {
		
		DefaultHttpClient httpclient = new DefaultHttpClient();
		
		try {
			
			httpclient.getCredentialsProvider().setCredentials(
                    new AuthScope(acp_http_ip, Integer.parseInt(acp_http_port)),
                    new UsernamePasswordCredentials(acp_http_auth_user, acp_http_auth_pw));
			
			HttpPost httppost = new HttpPost(acp_http_rtml_upload_url);

			FileBody bin = new FileBody(new File(file));
			StringBody comment = new StringBody("A binary file of some kind");
			StringBody autoSubmitValue = new StringBody("true");

			MultipartEntity reqEntity = new MultipartEntity();
			reqEntity.addPart("bin", bin);
			reqEntity.addPart("comment", comment);
			reqEntity.addPart("autoSubmit", autoSubmitValue);

			httppost.setEntity(reqEntity);

			System.out.println("executing request " + httppost.getRequestLine());
			HttpResponse response = httpclient.execute(httppost);
			HttpEntity resEntity = response.getEntity();

			System.out.println("----------------------------------------");
			System.out.println(response.getStatusLine());
			if (resEntity != null) {
				System.out.println("Response content length: " + resEntity.getContentLength());
			}
			
			EntityUtils.consume(resEntity);
			
		}catch(Exception ex){
			ex.printStackTrace();
			throw ex;
		}
		finally {
			try {
				httpclient.getConnectionManager().shutdown();
			} catch (Exception ignore) {
			}
		}
	}

}
