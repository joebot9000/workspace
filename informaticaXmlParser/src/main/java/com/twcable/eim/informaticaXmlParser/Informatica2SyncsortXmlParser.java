package com.twcable.eim.informaticaXmlParser;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.w3c.dom.Node;
import org.xml.sax.SAXException;

import java.io.File;
import java.io.IOException;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

public class Informatica2SyncsortXmlParser {

	private static org.apache.log4j.Logger log = Logger
			.getLogger(Informatica2SyncsortXmlParser.class);
	private static String inputFile;
	private static String overrideDateFormat;
	private static Boolean overrideDate;
	private static String outputString;

	public static void main(String argv[]) {

		try {
			init(argv);
			run();
			// wrapUp()

		} catch (Exception e) {
			System.out.println(e.getMessage());
		}
	}

	private static void init(String[] argv) {

		// log4j config
		BasicConfigurator.configure();
		String log4jConfPath = "/informaticaXmlParser/log4j.properties";
		PropertyConfigurator.configure(log4jConfPath);

		// set variables
		inputFile = argv[0].toString();
		overrideDate = Boolean.parseBoolean(argv[1].toString());
		overrideDateFormat = argv[2].toString();
	}

	private static void run() throws ParserConfigurationException,
			SAXException, IOException {
		File xmlFile = new File(inputFile);

		DocumentBuilderFactory docFactory = DocumentBuilderFactory
				.newInstance();
		DocumentBuilder docBuilder = docFactory.newDocumentBuilder();
		Document doc = docBuilder.parse(xmlFile);

		doc.getDocumentElement().normalize();

		// extract a list of nodes that contain the source tags
		NodeList nListSource = doc.getElementsByTagName("SOURCE");
		for (int nListSourceIndex = 0; nListSourceIndex <= nListSource
				.getLength(); nListSourceIndex++) {
			try {
				processTopLevelNode(nListSource.item(nListSourceIndex), "src");
			} catch (Exception e) {
				log.fatal(e.getStackTrace());
			}
		}

		// extract a list of nodes that contain the target tags
		NodeList nListTarget = doc.getElementsByTagName("TARGET");
		for (int nListTargetIndex = 0; nListTargetIndex <= nListTarget
				.getLength(); nListTargetIndex++) {
			try {
				processTopLevelNode(nListTarget.item(nListTargetIndex), "tgt");
			} catch (Exception e) {
				log.fatal(e.getStackTrace());
			}
		}
	}

	private static void processTopLevelNode(Node nTopLevelNode, String prefix) {
		int nodeIndex = 0;
		NodeList childNodes = nTopLevelNode.getChildNodes();
		outputString = "";

		// iterate through the children looking for nodes we care about
		for (nodeIndex = 0; nodeIndex <= childNodes.getLength(); nodeIndex++) {
			Node currentNode = childNodes.item(nodeIndex);

			// we need the flatfile node to determine delimiter
			if (currentNode.getNodeName().contains("FLATFILE")) {

				if (currentNode.getAttributes().getNamedItem("DELIMITED")
						.getNodeValue().toString().toLowerCase().equals("yes")) {
					System.out.println("/DELIMITEDRECORDLAYOUT");
					System.out.println(prefix
							+ "_"
							+ currentNode.getParentNode().getAttributes()
									.getNamedItem("NAME").getNodeValue()
									.toString().toLowerCase());
					System.out.println("{");
				}
			}

			// we need the field node (it will be either SOURCEFIELD or TARGET
			// field. Lazy programming
			if (currentNode.getNodeName().toString().toLowerCase()
					.contains("field")) {
				// this contains the column name
				String fieldName = currentNode.getAttributes()
						.getNamedItem("NAME").getNodeValue().toString()
						.toLowerCase();

				// this contains the data type
				String dataType = currentNode.getAttributes()
						.getNamedItem("DATATYPE").getNodeValue().toString()
						.toLowerCase();

				// check if the user wants to override xml data type.
				if (fieldName.toString().toLowerCase().contains("dte")
						&& overrideDate) {
					// example (YEAR-MM0-DD0)
					dataType = "DATETIME " + overrideDateFormat;
				}

				// this contains null/notnull options
				String nullable = currentNode.getAttributes()
						.getNamedItem("NULLABLE").getNodeValue().toString()
						.toLowerCase();

				// convert informatica data type to syncsort. need to add other
				// data types in future
				if (dataType.equals("string")) 
				{
					dataType = "CHARACTER ENCODING LOCALE";
				} 
				else if (dataType.equals("number")) {
					dataType = "EN";
				}
				else if (dataType.equals("datetime")) {
					dataType = "DATETIME " + overrideDateFormat;
				}

				// convert informatica null options to syncsort
				if (nullable.equals("null")) {
					nullable = "NULLIFEMPTY";
				} else {
					nullable = "NOTNULLABLE";
				}

				// build the final output string
				outputString += "\t" + fieldName + " " + dataType + " "
						+ nullable + ",\n";

			}

			// if we have finished processing the children then write the
			// outputString to stdout
			if (currentNode.getParentNode().getLastChild() == currentNode) {
				System.out.println(outputString.toString().substring(0,
						outputString.length() - 2)
						+ "\n}\n\n");
			}
		}

	}

}