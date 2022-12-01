import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Properties;
import java.util.Vector;

public class DBApp implements DBAppInterface {

	private int maxRowsInPage;
	private int maxKeysInIndexBucket;
	private ArrayList<String> tableNamesAddress = new ArrayList<String>();
	private Table currentTable;

	public DBApp() {
		init();
	}

	@Override
	public void init() {
		// initialization of the engine
		try {
			readConfig();
			File f = new File("src\\main\\resources\\data");
			f.mkdir();
			ArrayList<String> temp = decerializeTN();
			tableNamesAddress = new ArrayList<String>();
			createCSVHeaders();

		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	private void readConfig() {

		// reading the config file while tells the number of max rows and max indeces
		// per page
		Properties prop = new Properties();
		String fileName = "src\\main\\resources\\DBApp.config";
		FileInputStream is = null;
		try {
			is = new FileInputStream(fileName);
		} catch (FileNotFoundException ex) {

		}
		try {
			prop.load(is);
		} catch (IOException ex) {

		}

		maxRowsInPage = Integer.parseInt(prop.getProperty("MaximumRowsCountinPage"));
		maxKeysInIndexBucket = Integer.parseInt(prop.getProperty("MaximumKeysCountinIndexBucket"));
//		System.out.println("max rows per page is " + maxRowsInPage);
//		System.out.println("max indixes is " + maxKeysInIndexBucket);
	}

	private void createCSVHeaders() throws IOException {
		// create the table headers in the csv file
		File file = new File("src\\main\\resources\\metadata.csv");
		if (file.length() == 0) {
			FileWriter csvWriter = new FileWriter("src\\main\\resources\\metadata.csv");
			csvWriter.append("Table Name");
			csvWriter.append(",");
			csvWriter.append("Column Name");
			csvWriter.append(",");
			csvWriter.append("Column Type");
			csvWriter.append(",");
			csvWriter.append("ClusteringKey");
			csvWriter.append(",");
			csvWriter.append("Indexed");
			csvWriter.append(",");
			csvWriter.append("min");
			csvWriter.append(",");
			csvWriter.append("max");
			csvWriter.append("\n");
			csvWriter.flush();
			csvWriter.close();

		}
	}

	private void serializeTN() {
		try {

			FileOutputStream fileOut = new FileOutputStream("src\\main\\resources\\data\\engine_info");

			ObjectOutputStream out = new ObjectOutputStream(fileOut);

			out.writeObject(tableNamesAddress);

			out.close();

			fileOut.close();

		} catch (IOException i) {
			i.printStackTrace();
		}
	}

	private ArrayList<String> decerializeTN() {
		// TN for table names
		ArrayList<String> TN = null;
		try {
			FileInputStream fileIn = new FileInputStream("src\\main\\resources\\data\\engine_info");
			ObjectInputStream in = new ObjectInputStream(fileIn);
			TN = (ArrayList<String>) in.readObject();
			in.close();
			fileIn.close();
		} catch (IOException | ClassNotFoundException i) {
			return null;
		}
		return TN;
	}

	private void serializeTable(Table TName) {
		try {

			FileOutputStream fileOut = new FileOutputStream("src\\main\\resources\\data\\" + TName.name + ".full");

			ObjectOutputStream out = new ObjectOutputStream(fileOut);

			out.writeObject(TName);

			out.close();

			fileOut.close();

		} catch (IOException i) {
			i.printStackTrace();
		}
	}

	private Table decerializeTable(String tableName) {
		Table table = null;
		try {
			FileInputStream fileIn = new FileInputStream("src\\main\\resources\\data\\" + tableName + ".full");
			ObjectInputStream in = new ObjectInputStream(fileIn);
			table = (Table) in.readObject();
			in.close();
			fileIn.close();
		} catch (IOException | ClassNotFoundException i) {
			return null;
		}
		return table;
	}

//------------------------------------ main methods/functionality

	@Override
	public void createTable(String tableName, String clusteringKey, Hashtable<String, String> colNameType,
			Hashtable<String, String> colNameMin, Hashtable<String, String> colNameMax) throws DBAppException {
		Table created = new Table(tableName, maxRowsInPage);
		FileWriter csvWriter = null;
		try {
			csvWriter = new FileWriter("src\\main\\resources\\metadata.csv", true);
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		try {

			for (int i = 0; i < tableNamesAddress.size(); i++) {
				if (tableName.equals(tableNamesAddress.get(i))) {
					throw new DBAppException("There is an existing table with the same name");
				}
			}

			// you need to make sure everything is correct before filling the csv file:
			// check all sizes are equal else exception d
			// check data type is right d
			// check min and max are of the data type d
			// check true and false
			// check clustering key d

			Enumeration<String> Name = colNameType.keys();

			if (colNameType.size() != colNameMin.size())
				throw new DBAppException("You haven't enterd the minimum for all columns or entered more values");
			else if (colNameType.size() != colNameMax.size())
				throw new DBAppException("You haven't enterd the maximum for all columns or entered more values");
			else if (!colNameType.containsKey(clusteringKey)) {
				throw new DBAppException("make sure the clustering key data type is entered");
			}
			int counterForCluster = 0;
			while (Name.hasMoreElements()) {
				String currentName = Name.nextElement();
				String currentType = colNameType.get(currentName);
				Object currentMin = "";
				Object currentMax = "";
				currentMin = colNameMin.get(currentName);
				currentMax = colNameMax.get(currentName);

				switch (currentType) {
				case "java.lang.Integer":
					try {
						Integer.parseInt(currentMin + "");

					} catch (Exception e) {
						throw new DBAppException(currentMin + " is not an Integer");
					}
					try {
						Integer.parseInt(currentMax + "");
					} catch (Exception e) {
						throw new DBAppException(currentMax + " is not an Integer");
					}
					break;
				case "java.lang.Double":
					try {
						Double.parseDouble(currentMin + "");
					} catch (Exception e) {
						throw new DBAppException(currentMin + " is not Double");
					}
					try {
						Double.parseDouble(currentMax + "");
					} catch (Exception e) {
						throw new DBAppException(currentMax + " is not Double");
					}
					break;
				case "java.util.Date":
					try {

						new SimpleDateFormat("yyyy-mm-dd").parse(currentMin + "");
					} catch (Exception e) {
						throw new DBAppException(currentMin + " is not a vaild date");
					}
					try {

						new SimpleDateFormat("yyyy-mm-dd").parse(currentMax + "");
					} catch (Exception e) {
						throw new DBAppException(currentMax + " is not a vaild date");
					}
					break;
				case "java.lang.String":
					// note that instances like "ZYQ" can exist so the order of letter isn't handled
//					checkStringFormat(currentMin + "");
//					checkStringFormat(currentMax + "");
					break;
				default:
					throw new DBAppException(currentType + " isn't int,double,String nor date");
				}
				csvWriter.append(tableName);
				csvWriter.append(",");
				csvWriter.append(currentName);
				csvWriter.append(",");
				csvWriter.append(currentType);
				csvWriter.append(",");

				if (clusteringKey == currentName) {
					csvWriter.append("True");
					created.clusteringKeyIndex = counterForCluster;
					created.clusterKeyType = currentType;
				} else
					csvWriter.append("False");
				counterForCluster++;
				csvWriter.append(",");
				csvWriter.append("False");
				csvWriter.append(",");

				csvWriter.append(currentMin + "");
				csvWriter.append(",");
				csvWriter.append(currentMax + "");
				csvWriter.append("\n");
			}
			tableNamesAddress.add(tableName);
			serializeTN();
			serializeTable(created);
			csvWriter.flush();
			csvWriter.close();
		} catch (IOException e) {
			throw new DBAppException("Cannot read metadata file");
		}
	}

	@Override
	public void insertIntoTable(String tableName, Hashtable<String, Object> colNameValue) throws DBAppException {
		// TODO Auto-generated method stub

		if (checkAllTablesNamesFor(tableName) == null) {
			throw new DBAppException("The table doesn't exist in the DB");
		}

		try {
			ArrayList<String[]> columnsInfo = getColumnInfo(tableName);
			// 2.validate the methods input are correct
			validateInputs(tableName, columnsInfo, colNameValue);

			// figure to which table we insert
			currentTable = decerializeTable(checkAllTablesNamesFor(tableName));

			ArrayList<String> record = new ArrayList<String>(columnsInfo.size() - 1);
			int clusteringKey = 0;
			String clusteringKeyDataType = null;
			for (int i = 0; i < columnsInfo.size(); i++) {
				if (columnsInfo.get(i)[0].equals(tableName)) {
					if (columnsInfo.get(i)[2].equals("java.util.Date"))
						record.add(processDate(colNameValue.get(columnsInfo.get(i)[1]) + ""));
					else
						record.add(colNameValue.get(columnsInfo.get(i)[1]) + "");
					if (columnsInfo.get(i)[3].equalsIgnoreCase("True")) {
						clusteringKey = i;
						clusteringKeyDataType = columnsInfo.get(i)[2];
//						break;
					}
				}
			}

			int pageNum = 0;
			if (currentTable.pageAddress.size() == 0) {
				pageNum = -1;// i.e. doesn't exist

			} else if (currentTable.pageAddress.size() == 1) {

				pageNum = 0;
				if (currentTable.count.get(0) < maxRowsInPage) {
					pageNum = 0;
				} else {

					if ((compare((colNameValue.get(columnsInfo.get(clusteringKey)[5]) + ""),
							currentTable.max.get(0))) < 0)
						pageNum = 0;

					else
						pageNum = 1;
				}
			}

			else {

				for (int i = 0; i < currentTable.pageAddress.size() - 1; i++) {
					// first check compare with min & max in single page i
					if (recordInBetween(columnsInfo.get(clusteringKey)[5], currentTable.max.get(i) + "",
							record.get(clusteringKey), clusteringKeyDataType)) {
						pageNum = i;
						break;
					} // compare the max of cuurent page with the min of next page
					else if (recordInBetween(currentTable.max.get(i) + "", currentTable.min.get(i + 1) + "",
							record.get(clusteringKey), clusteringKeyDataType)) {
						if (currentTable.count.get(i) < maxRowsInPage) { // if current page has place
							pageNum = i;
						} else {
							pageNum = i + 1;
						}
					}
					// else loop
					else {
						pageNum = i + 1;
					}

				}

			}

//			System.out.print(record + " is in ");
//			System.out.println(
//					pageNum == -1 ? "first page" : "page num " + pageNum + "count now is:" + currentTable.count.size());
			currentTable.insertInPage(pageNum, record);
			serializeTable(currentTable);

		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	@Override
	public void updateTable(String tableName, String clusteringKeyValue, Hashtable<String, Object> columnNameValue)
			throws DBAppException {

		if (checkAllTablesNamesFor(tableName) == null) {
			throw new DBAppException("The table doesn't exist in the DB");
		}

		ArrayList<String[]> columnsInfo = null;
		try {
			columnsInfo = getColumnInfo(tableName);
		} catch (IOException e) {

			e.printStackTrace();
		}
		if (!checkModCol(columnNameValue, columnsInfo)) {
			throw new DBAppException("The columns you want to update does not exist in the specified table");
		}

		for (int i = 0; i < columnsInfo.size(); i++) {
			if (!checkValueCompatibility(columnsInfo.get(i)[2], columnsInfo.get(i)[5], columnsInfo.get(i)[6],
					columnNameValue.get(columnsInfo.get(i)[1]) + "")) {
				throw new DBAppException("not compatible data type in column " + columnsInfo.get(i)[1]);
			}
		}

		// specify which table
		currentTable = decerializeTable(tableName);
		// specify type of clustering key
		String clusterType = getClusterType(columnsInfo);
		String clusterName = getClusterName(columnsInfo);
		if (columnNameValue.containsKey(clusterName)) {
			throw new DBAppException("You are not allowed to change the value of " + clusterName);
		}
		// find page with the record
		Vector<ArrayList<String>> page = recordPage(currentTable, clusteringKeyValue, clusterType);
		if (page == null) {
			throw new DBAppException("There is no such a record");
		}
		// find index of the record in the specified page

		int recordIndex = currentTable.binarySearch(page, clusteringKeyValue, clusterType)[0];

		if (recordIndex == -1) {
			throw new DBAppException("There is no such a record");
		}
		int c = 0;
		for (int k = 0; k < columnsInfo.size(); k++) {
			String value = "" + columnNameValue.get(columnsInfo.get(k)[1]);
			if (value.equals("null")) {
				c--;
				continue;
			} else {
				page.get(recordIndex).set(k + c, value);
			}
		}
		int pageIndex = getPageIndex(currentTable, clusteringKeyValue, clusterType);
		currentTable.serialize(page, currentTable.pageAddress.get(pageIndex));
		serializeTable(currentTable);

	}

	@Override
	public void deleteFromTable(String tableName, Hashtable<String, Object> columnNameValue) throws DBAppException {
		try {
			if (checkAllTablesNamesFor(tableName) == null) {
				throw new DBAppException("The table doesn't exist in the DB");
			}

			ArrayList<String[]> columnsInfo = getColumnInfo(tableName);

			if (!checkModCol(columnNameValue, columnsInfo)) {
				throw new DBAppException("The columns you want to update does not exist in the specified table");
			}

			for (int i = 1; i < columnsInfo.size(); i++) {
				if (!checkValueCompatibility(columnsInfo.get(i)[2], columnsInfo.get(i)[5], columnsInfo.get(i)[6],
						columnNameValue.get(columnsInfo.get(i)[1]) + "")) {
					throw new DBAppException("not compatible data type in column " + columnsInfo.get(i)[1]);
				}
			} // specify which table

			currentTable = decerializeTable(tableName);
			String clusterName = getClusterName(columnsInfo);
			String clusterType = getClusterType(columnsInfo);
			String clusteringKeyValue = "" + columnNameValue.get(clusterName);
			int c = 0;
			// find page with the record

			if (clusteringKeyValue.equals("null")) {
				for (int i = 0; i < currentTable.pageAddress.size(); i++) {
					Vector<ArrayList<String>> page = currentTable.decerialize(currentTable.pageAddress.get(i));
					for (int j = 0; j < page.size(); j++) {
						if (equality(columnNameValue, page.get(j))) {
							c++;
							page.remove(j);
							j--;
							currentTable.count.set(i, currentTable.count.get(i) - 1);
						}
					}
					if (page.isEmpty()) {
						currentTable.pageAddress.remove(currentTable.pageAddress.get(i));
						currentTable.max.remove(i);
						currentTable.min.remove(i);
						currentTable.count.remove(i);
						i--;
					} else {
						currentTable.min.set(i, page.get(0).get(getIndexOfColumnInTable(clusterName, columnsInfo)));
						currentTable.max.set(i,
								page.get(page.size() - 1).get(getIndexOfColumnInTable(clusterName, columnsInfo)));
						currentTable.serialize(page, currentTable.pageAddress.get(i));
					}
				}
				if (c == 0) {
					throw new DBAppException("There is no such a record");
				} else {
//					System.out.println(c + " deleted Record/s");
				}
			} else {
				Vector<ArrayList<String>> page = recordPage(currentTable, clusteringKeyValue, clusterType);
				// page index is used to know the min and max value of the page of the cluster
				// key
				int pageIndex = getPageIndex(currentTable, clusteringKeyValue, clusterType);

				// index of the record in the specified page
				if (page == null) {
					throw new DBAppException("There is no such a record");
				}

				// find index of the record in the specified page

				int recordIndex = currentTable.binarySearch(page, clusteringKeyValue, clusterType)[0];
				if (recordIndex == -1) {
					throw new DBAppException("There is no such a record");
				}

				// used to update min and max of the page
				for (int i = 0; i < columnsInfo.size(); i++) {
					if (!(columnNameValue.get(columnsInfo.get(i)[1]) + "").equals(page.get(recordIndex).get(i))) {
						throw new DBAppException("There is no such a record");
					}
				}
				int oldPageSize = page.size();
				page.remove(recordIndex);
				currentTable.count.set(pageIndex, currentTable.count.get(pageIndex) - 1);
				if (page.isEmpty()) {
					currentTable.count.remove(pageIndex);
					currentTable.min.remove(pageIndex);
					currentTable.max.remove(pageIndex);
					currentTable.pageAddress.remove(pageIndex);

				}
				// check if the deleted record was the first or the last record in the page.
				// In other words, check if the clustering value of the record was minimum or
				// maximum value of the page
				else {
					if (recordIndex == oldPageSize - 1) {
						currentTable.max.set(pageIndex,
								page.get(page.size() - 1).get(getIndexOfColumnInTable(clusterName, columnsInfo)));
					} else {
						if (recordIndex == 0) {
							currentTable.min.set(pageIndex,
									page.get(0).get(getIndexOfColumnInTable(clusterName, columnsInfo)));
						}
					}
					currentTable.serialize(page, currentTable.pageAddress.get(pageIndex));
				}
			}
			serializeTable(currentTable);
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
	}

	@Override
	public void createIndex(String tableName, String[] columnNames) throws DBAppException {
		// TODO Auto-generated method stub

	}

	@Override
	public Iterator selectFromTable(SQLTerm[] sqlTerms, String[] arrayOperators) throws DBAppException {
		// TODO Auto-generated method stub
		return null;
	}

	// --------------------------------- helper methods
	private boolean checkValueCompatibility(String dataType, String min, String max, String enteredVal) {

		if (enteredVal.equals("null")) {
			return true;
		}

		switch (dataType) {
		case "java.lang.Integer":
			int valInt;
			try {
				valInt = Integer.parseInt(enteredVal);
			} catch (Exception e) {
				return false;
			}
			int minInt = Integer.parseInt(min);
			int maxInt = Integer.parseInt(max);
			if (valInt > maxInt || valInt < minInt)
				return false;
			break;
		case "java.lang.Double":
			double valDouble;
			try {
				valDouble = Double.parseDouble(enteredVal);
			} catch (Exception e) {
				return false;
			}
			double minDouble = Double.parseDouble(min);
			double maxDouble = Double.parseDouble(max);
			if (valDouble > maxDouble || valDouble < minDouble)
				return false;
			break;
		case "java.util.Date":
			String valDate;
			try {
				valDate = processDate(enteredVal);
				if (max.compareTo(valDate) < 0 || min.compareTo(valDate) > 0)
					return false;

			} catch (Exception e) {
				return false;
			}
			break;
		default:
			// string case
			if (enteredVal.length() > max.length())
				return false;
		}

		return true;
	}

	private void validateInputs(String tableName, ArrayList<String[]> columnsInfo,
			Hashtable<String, Object> colNameValue) throws DBAppException {
		boolean clusteringKeyExists = false;
		for (int i = 0; i < columnsInfo.size(); i++) {
			if (columnsInfo.get(i)[0].equals(tableName)) {

				if (colNameValue.size() > columnsInfo.size())
					throw new DBAppException("the number of provided paramaters don't match that of the thable");
				// check if clustering key is available

				if (columnsInfo.get(i)[3].equalsIgnoreCase("True")) {
					if (colNameValue.containsKey(columnsInfo.get(i)[1]))
						clusteringKeyExists = true;
				}

				// a. the column names
				if (colNameValue.containsKey(columnsInfo.get(i)[1])) {
					// b. the value is compatible with the data type and the min/max
					if (!checkValueCompatibility(columnsInfo.get(i)[2], columnsInfo.get(i)[5], columnsInfo.get(i)[6],
							colNameValue.get(columnsInfo.get(i)[1]) + ""))
						throw new DBAppException("the entered data isn't compatible with the table.\n"
								+ "The entered data should be between " + columnsInfo.get(i)[5] + " and "
								+ columnsInfo.get(i)[6] + " and be of type " + columnsInfo.get(i)[2]);
				}
			}

		}
		if (!clusteringKeyExists)
			throw new DBAppException("clustering key isn't provided in input");
	}

	private String processDate(String s) {
		// "Mon Sep 28 00:00:00 EEST 1992" -> yyyy-MM-dd

		String y = s.substring(s.length() - 4, s.length());
		String d = s.substring(8, 10);
		String m = s.substring(4, 7);
		switch (s.substring(4, 7)) {
		case "Jan":
			m = "01";
			break;
		case "Feb":
			m = "02";
			break;
		case "Mar":
			m = "03";
			break;
		case "Apr":
			m = "04";
			break;
		case "May":
			m = "05";
			break;
		case "Jun":
			m = "06";
			break;
		case "Jul":
			m = "07";
			break;
		case "Aug":
			m = "08";
			break;
		case "Sep":
			m = "09";
			break;
		case "Oct":
			m = "10";
			break;
		case "Nov":
			m = "11";
			break;
		case "Dec":
			m = "12";
			break;
		}
		return y + "-" + m + "-" + d;
	}

	private ArrayList<String[]> getColumnInfo(String tableName) throws IOException {
		BufferedReader csvReader = new BufferedReader(new FileReader("src\\main\\resources\\metadata.csv"));
		String row = "";

		ArrayList<String[]> columnsInfo = new ArrayList<String[]>();
		while ((row = csvReader.readLine()) != null) {
			String[] info = row.split(",");
			if (info[0].equals(tableName)) {
				columnsInfo.add(info);
			}
		}
		csvReader.close();
		return columnsInfo;
	}

	private Vector<ArrayList<String>> recordPage(Table table, String clusteringKeyValue, String clusterType) {
		Vector<ArrayList<String>> page = null;
		for (int i = 0; i < currentTable.pageAddress.size(); i++) {
			if (recordInBetween(currentTable.min.get(i), currentTable.max.get(i), clusteringKeyValue, clusterType)) {
				page = currentTable.decerialize(currentTable.pageAddress.get(i));
				break;
			}
		}
		return page;
	}

	private boolean recordInBetween(String min, String max, String data, String dataType) {

		switch (dataType) {
		case "java.lang.Integer":

			int dataInt = Integer.parseInt(data);
			int minInt = Integer.parseInt(min);
			int maxInt = Integer.parseInt(max);
			if (maxInt >= dataInt && dataInt >= minInt)
				return true;
			else
				return false;
		case "java.lang.Double":

			double dataDouble = Double.parseDouble(data);
			double minDouble = Double.parseDouble(min);
			double maxDouble = Double.parseDouble(max);
			if (maxDouble >= dataDouble && dataDouble >= minDouble)
				return true;
			else
				return false;

		case "java.util.Date":
			try {
				FileWriter myWriter = new FileWriter("filename.txt", true);
				myWriter.write(max + " " + min + " " + data);
				myWriter.append(System.lineSeparator());
				myWriter.append(System.lineSeparator());

				myWriter.close();
			} catch (Exception e) {

			}
			if (compare(max, data) >= 0 && compare(data, min) >= 0)
				return true;
			return false;

		default:
			// string case
			if (compare(max, data) >= 0 && compare(data, min) >= 0)
				return true;
			else
				return false;

		}
	}

	private int compare(String s, String ss) {

		while (s.length() > 1) {
			if (s.charAt(0) == '0')
				s = s.substring(1);
			else
				break;
		}
		while (ss.length() > 1) {
			if (ss.charAt(0) == '0')
				ss = ss.substring(1);
			else
				break;
		}
		if (s.length() == ss.length())
			return s.compareTo(ss);

		return (s.length() > ss.length()) ? 1 : -1;
	}

	private String checkAllTablesNamesFor(String s) {
		ArrayList<String> temp = decerializeTN();
		tableNamesAddress = (temp == null) ? (new ArrayList<String>()) : temp;
		for (int i = 0; i < tableNamesAddress.size(); i++) {
			if (tableNamesAddress.get(i).equals(s)) {
				tableNamesAddress = new ArrayList<String>();
				return s;
			}

		}
		tableNamesAddress = new ArrayList<String>();

		return null;
	}

	private boolean checkModCol(Hashtable<String, Object> columnNameValue, ArrayList<String[]> columnsInfo) {
		// check if updated, inserted rows are have the same columns as in the table
		ArrayList<String> tableCol = new ArrayList<String>();
		for (int i = 0; i < columnsInfo.size(); i++) {
			tableCol.add(columnsInfo.get(i)[1]);
		}
		Enumeration<String> updatedCol = columnNameValue.keys();
		while (updatedCol.hasMoreElements()) {
			String colName = updatedCol.nextElement();
			if (!tableCol.contains(colName)) {
				return false;
			}
		}
		return true;
	}

	private boolean equality(Hashtable<String, Object> columnNameValue, ArrayList<String> record) throws IOException {
		ArrayList<String[]> columnsinfo = getColumnInfo(currentTable.name);
		Enumeration<String> Keys = columnNameValue.keys();
		while (Keys.hasMoreElements()) {
			String key = Keys.nextElement();
			String s1 = columnNameValue.get(key) + "";
			String s2 = record.get(getIndexOfColumnInTable(key, columnsinfo));
			if (!((columnNameValue.get(key)) + "").equals(record.get(getIndexOfColumnInTable(key, columnsinfo)))) {
				return false;
			}
		}
		return true;
	}

	private int getIndexOfColumnInTable(String columnName, ArrayList<String[]> columnsinfo) {
		int index = 0;
		for (int i = 0; i < columnsinfo.size(); i++) {
			if (columnName.equals(columnsinfo.get(i)[1])) {
				index = i;
				break;
			}
		}
		return index;
	}

	private String getClusterName(ArrayList<String[]> columnsInfo) {
		String clusterName = null;
		for (int i = 0; i < columnsInfo.size(); i++) {
			if (columnsInfo.get(i)[3].equalsIgnoreCase("True")) {
				clusterName = columnsInfo.get(i)[1];
				break;
			}
		}
		return clusterName;
	}

	private String getClusterType(ArrayList<String[]> columnsInfo) {
		String clusterType = null;
		for (int i = 0; i < columnsInfo.size(); i++) {
			if (columnsInfo.get(i)[3].equalsIgnoreCase("True")) {
				clusterType = columnsInfo.get(i)[2];
				break;
			}
		}
		return clusterType;
	}

	private int getPageIndex(Table table, String clusteringKeyValue, String clusterType) {
		int i = 0;
		for (i = 0; i < currentTable.pageAddress.size(); i++) {
			if (recordInBetween(currentTable.min.get(i), currentTable.max.get(i), clusteringKeyValue, clusterType)) {
				break;
			}
		}
		return i;
	}

	private void cleanStart() {

		File index = new File("src\\main\\resources\\data");
		String[] entries = index.list();
		for (String s : entries) {
			File currentFile = new File(index.getPath(), s);
			currentFile.delete();
		}
		index.delete();

		new File("src\\main\\resources\\metadata.csv").delete();

		tableNamesAddress.clear();
		init();

	}

	// -----------------------------------main method
	public static void main(String[] args) throws DBAppException {
		DBApp dpa = new DBApp();
		Hashtable colNameType = new Hashtable();
		colNameType.put("id", "java.lang.Integer");
		colNameType.put("name", "java.lang.String");
		colNameType.put("gpa", "java.lang.Double");
		colNameType.put("date", "java.util.Date");

		Hashtable min = new Hashtable();
		min.put("id", new Integer(0));
		min.put("gpa", new Double(0));
		min.put("name", "AAAAAA");
		min.put("date", "2000-01-01");

		Hashtable max = new Hashtable();
		max.put("id", 50);
		max.put("name", "ZZZZZZ");
		max.put("gpa", new Double(500));
		max.put("date", "3000-10-20");

		dpa.cleanStart();
//		dpa.createTable("a", "id", colNameType, min, max);
//		
//		dpa.createTable("b", "name", colNameType, min, max);
//		
//		dpa.createTable("c", "gpa", colNameType, min, max);
//		
//		dpa.createTable("d", "date", colNameType, min, max);
//		
//		Hashtable<String, Object> ht = new Hashtable<String, Object>();
//		ArrayList<String> temp = dpa.decerializeTN();
//		ArrayList<String> tableNamesAddress = (temp == null) ? (new ArrayList<String>()) : temp;
//		ht.clear();
//		ht.put("id", 7);
//		ht.put("gpa", 3);
//		ht.put("name","a");
//		ht.put("date",new Date(2000-1900,10-1,20));
//		dpa.insertIntoTable("a", ht);
//		System.out.println(dpa.decerializeTable(tableNamesAddress.get(0)));
////		
//		ht.clear();
//		ht.put("id", 7);
//		ht.put("gpa", 3);
//		ht.put("name","b");
//		ht.put("date",new Date(2000-1900,10-1,20));
//		dpa.insertIntoTable("b", ht);
//		System.out.println(dpa.decerializeTable(tableNamesAddress.get(1)));
	}

}
