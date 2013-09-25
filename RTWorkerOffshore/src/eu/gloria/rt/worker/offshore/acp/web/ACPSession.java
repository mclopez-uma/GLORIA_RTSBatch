package eu.gloria.rt.worker.offshore.acp.web;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;

public class ACPSession {
	
	private String indexURL;
	private String host;
	private int port;
	private String user;
	private String pw;
	
	public ACPSession(String indexURL, String host, int port, String user, String pw){
		
		this.indexURL = indexURL;
		this.host = host;
		this.port = port;
		this.user = user;
		this.pw = pw;
	}
	
	public void open() throws Exception{
		
		DefaultHttpClient httpclient = new DefaultHttpClient();
		
		try {
			
			httpclient.getCredentialsProvider().setCredentials(
                    new AuthScope(host, port),
                    new UsernamePasswordCredentials(user, pw));
			
			HttpGet httpGet = new HttpGet(indexURL);
			
			

			System.out.println("ACPSession. open()" + httpGet.getRequestLine());
			HttpResponse response = httpclient.execute(httpGet);
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
