package eu.gloria.rt.worker.offshore.acp.web;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.w3c.dom.ls.DOMImplementationLS;
import org.w3c.dom.ls.LSSerializer;
import org.xml.sax.SAXException;

/**
 * @author Administrator
 *
 */
public class Plan {

	private int id;

	/**
	 * Name of the plan
	 */
	private String name;

	/**
	 * Status of the plan
	 */
	private String status;

	/**
	 * Number of observations in the plan
	 */
	private int obs;


	/**
	 * Total number of images in all observations
	 */
	private int imgs;

	/**
	 * Total shutter-open time for all images in all observations
	 */
	private String time;

	/**
	 * Number of images in all observations remaining to be acquired
	 */
	private double act;

	/**
	 * Number of images in all observations that have been completed
	 */
	private double cmpl;

	/**
	 * Number of images in all observations which have failed
	 */
	private double fail;

	/**
	 * Number of images in all observations which are disabled
	 */
	private double dis;


	public String innerXml(Node node) {
		DOMImplementationLS lsImpl = (DOMImplementationLS)node.getOwnerDocument().getImplementation().getFeature("LS", "3.0");
		LSSerializer lsSerializer = lsImpl.createLSSerializer();
		NodeList childNodes = node.getChildNodes();
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < childNodes.getLength(); i++) {
			sb.append(lsSerializer.writeToString(childNodes.item(i)));
		}
		return sb.toString(); 
	}

	/**
	 * Crea un objeto plan a partir de una fila de una tabla HTML
	 * @param nodoTR
	 */
	public Plan(Node nodoTR)
	{
		NodeList tableDivisor;

		tableDivisor = ((Element)nodoTR).getElementsByTagName("td");
		int cantCampos;
		cantCampos = tableDivisor.getLength();
		if (cantCampos == 9)
		{
			for (int i = 0; i < 9; i++)
			{                    
				String innText;
				Node nodoSeparador;
				nodoSeparador = tableDivisor.item(i);
				innText = nodoSeparador.getTextContent();//nodoSeparador.InnerText;
				//System.out.println(String.format("field[%d]=%s", i, innText));

				switch (i)
				{
				case 0:
					this.name = innText;
					Pattern patron;
					Matcher calce;
					patron = Pattern.compile("\\((\\d)*\\)");
					calce = patron.matcher(this.innerXml(nodoSeparador));
					String strID;
					if (calce.find())
					{
						String strCalce;
						strCalce = calce.group(); 
						//System.out.println("strCalce="+strCalce);
						strID=  strCalce.substring(1, strCalce.length()-1);
						this.id = Integer.parseInt(strID);
					}
					break;
				case 1:
					this.status = innText;
					break;
				case 2:
					this.obs = Integer.parseInt(innText);
					break;
				case 3:
					this.imgs = Integer.parseInt(innText);
					break;
				case 4:
					this.time = innText;
					break;
				case 5:
					this.act = Parse_Number(innText);
					break;
				case 6:
					this.cmpl = Parse_Number(innText);
					break;
				case 7:
					this.fail = Parse_Number(innText);
					break;
				case 8:
					this.dis = Parse_Number(innText);
					break;

				default:
					break;
				}
			}
		}
	}
	
	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getStatus() {
		return status;
	}

	public void setStatus(String status) {
		this.status = status;
	}

	public int getObs() {
		return obs;
	}

	public void setObs(int obs) {
		this.obs = obs;
	}

	public int getImgs() {
		return imgs;
	}

	public void setImgs(int imgs) {
		this.imgs = imgs;
	}

	public String getTime() {
		return time;
	}

	public void setTime(String time) {
		this.time = time;
	}

	public double getAct() {
		return act;
	}

	public void setAct(double act) {
		this.act = act;
	}

	public double getCmpl() {
		return cmpl;
	}

	public void setCmpl(double cmpl) {
		this.cmpl = cmpl;
	}

	public double getFail() {
		return fail;
	}

	public void setFail(double fail) {
		this.fail = fail;
	}

	public double getDis() {
		return dis;
	}

	public void setDis(double dis) {
		this.dis = dis;
	}

	private static double Parse_Number(String texto)
	{
		double respuesta;
		respuesta = -1;
		if (!(texto.equals("---")))
		{
			if (texto.endsWith("%")){
				String sinPorcentaje;
				sinPorcentaje = texto.substring(0, texto.length()-1);
				//System.out.println("sinPorcentaje="+sinPorcentaje);
				respuesta = Double.parseDouble(sinPorcentaje);
				
			}else
			{
				respuesta = Double.parseDouble(texto);
			}
		}
		return respuesta;
	}	

	@Override
	public String toString() {
		return  (String.format("%d %s",this.id,this.name));
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		Plan testPlan;
		DocumentBuilderFactory docBuilderFactory = DocumentBuilderFactory.newInstance();
		DocumentBuilder docBuilder;
		Document doc;
		docBuilder = null;
		try {
			docBuilder = docBuilderFactory.newDocumentBuilder();
		} catch (ParserConfigurationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		doc = null;
		try {
			doc = docBuilder.parse("plan.xml");
		} catch (SAXException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		testPlan = new Plan(doc.getDocumentElement());

		System.out.println("testplan="+testPlan);
	}

}