package tongji.graduation.wangkaile.tagmodel.input;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.Map.Entry;

import tongji.graduation.wangkaile.tagmodel.basic.object.WebObject;
import tongji.graduation.wangkaile.tagmodel.basic.object.WebSite;

public class FileInput {
	// ��������webObject����
	private static Hashtable<Integer, WebObject> webObjectSet;
	private static Hashtable<String, Integer> tagSet;
	private static Hashtable<Integer, WebSite> webSiteSet;
	// ����index��������TagModel��webObject��id
	private static int index = 0;
	private static int indexWebSite = 0;
	// ����indexTag��������TagModel��tag��id
	private static int indexTag = 0;

	public static void readFileByLines4Amazon(String fileName, int lable) {
		File file = new File(fileName);
		BufferedReader reader = null;
		int indexNow = 0;
		try {
			// System.out.println("��������������������������������������");
			reader = new BufferedReader(new FileReader(file));
			String tempString = null;
			int flag = 0;
			// ����������������������null����������
			WebObject tempObject = new WebObject(lable);
			tempObject.initializeProbability4UnlableObject();
			while ((tempString = reader.readLine()) != null) {
				// ������������������������������description������������
				if (tempString.trim().equals("") && flag != 1) {
					flag = 0;
					webObjectSet.put(index, tempObject);
					tempObject = new WebObject(lable);
					tempObject.initializeProbability4UnlableObject();
					index++;
					indexNow++;
//					if(indexNow > 5){
//						break;
//					}
					System.out.println("null string");
					continue;
				}
				// ������������tags
				if (flag == 2) {
					String[] strArray = tempString.split("	");
					// int tempInt = strArray.length;
					System.out.println("tagName: " + strArray[0]
							+ "	frequency: " + strArray[1]);
					String tempTag = strArray[0];
					int tempInt = indexTag;
					if (tagSet.containsKey(tempTag)) {
						tempInt = tagSet.get(tempTag);
					} else {
						tagSet.put(tempTag, tempInt);
						indexTag++;
					}

					double tempFrequency = Double.parseDouble(strArray[1]);
					tempObject.getTagList().put(tempInt, tempFrequency);
					continue;
				}
				// ����������id������
				if (flag == 0 || flag == 1) {
					switch (flag) {
					case 0:
						System.out.println("id: " + tempString);
						tempObject.setRealId(tempString);
						break;
					case 1:
						System.out.println("description: " + tempString);
						tempObject.setDescription(tempString);
						break;
					default:
						break;
					}
					flag++;
					continue;
				}
			}
			reader.close();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (reader != null) {
				try {
					reader.close();
				} catch (IOException e1) {
				}
			}
		}
	}

	public static void readFileByLines4Odp(String fileName, int lable) {
		File file = new File(fileName);
		BufferedReader reader = null;
		int indexNow = 0;
		try {
			// System.out.println("��������������������������������������");
			reader = new BufferedReader(new FileReader(file));
			String tempString = null;
			int flag = 0;
			// ����������������������null����������
			WebSite tempSite = new WebSite(lable);
			tempSite.initializeProbability();
			while ((tempString = reader.readLine()) != null) {
				// ������������������������������description������������
				if (tempString.trim().equals("")) {
					flag = 0;
					webSiteSet.put(indexWebSite, tempSite);
					tempSite = new WebSite(lable);
					tempSite.initializeProbability();
					indexWebSite++;
					indexNow++;
//					if(indexNow > 5){
//						break;
//					}
					System.out.println("null string");
					continue;
				}
				// ������������tags
				if (flag == 1) {
					String[] strArray = tempString.split("	");
					// int tempInt = strArray.length;
					System.out.println("tagName: " + strArray[0]
							+ "	frequency: " + strArray[1]);
					String tempTag = strArray[0];
					int tempInt = indexTag;
					if (tagSet.containsKey(tempTag)) {
						tempInt = tagSet.get(tempTag);
					} else {
						tagSet.put(tempTag, tempInt);
						indexTag++;
					}
					double tempFrequency = Double.parseDouble(strArray[1]);
					tempSite.getTagList().put(tempInt, tempFrequency);
					continue;
				}
				// ��������������
				if (flag == 0) {
					System.out.println("webSiteAddress: " + tempString);
					tempSite.setDescription(tempString);
					flag++;
					continue;
				}
			}
			reader.close();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (reader != null) {
				try {
					reader.close();
				} catch (IOException e1) {
				}
			}
		}
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		webObjectSet = new Hashtable<Integer, WebObject>();
		tagSet = new Hashtable<String, Integer>();
		webSiteSet = new Hashtable<Integer, WebSite>();
		// Amazon��������
		readFileByLines4Amazon("data/amazon/Books.txt", 0);
		readFileByLines4Amazon("data/amazon/Electronics.txt", 1);
		readFileByLines4Amazon("data/amazon/HealthPersonCare.txt", 2);
		readFileByLines4Amazon("data/amazon/HomeGarden.txt", 3);
		readFileByLines4Amazon("data/amazon/Jewelry.txt", 4);
		readFileByLines4Amazon("data/amazon/Music.txt", 5);
		readFileByLines4Amazon("data/amazon/OfficeProducts.txt", 6);
		readFileByLines4Amazon("data/amazon/PetSupplies.txt", 7);
		// odp��������
		readFileByLines4Odp("data/odp/Books.txt", 0);
		readFileByLines4Odp("data/odp/ConsumerElectronics.txt", 1);
		readFileByLines4Odp("data/odp/Health.txt", 2);
		readFileByLines4Odp("data/odp/HomeandGarden.txt", 3);
		readFileByLines4Odp("data/odp/Jewelry.txt", 4);
		readFileByLines4Odp("data/odp/Music.txt", 5);
		readFileByLines4Odp("data/odp/Office.txt", 6);
		readFileByLines4Odp("data/odp/Pet.txt", 7);
		System.out.println("Input Over");

		Hashtable<Integer, List<Double>> v1f = new Hashtable<Integer, List<Double>>();
		Hashtable<Integer, List<Double>> v2f = new Hashtable<Integer, List<Double>>();
		Hashtable<Integer, Integer> trueV2ClassDic = new Hashtable<Integer, Integer>();
		Hashtable<Integer, Hashtable<Integer, Double>> W13 = new Hashtable<Integer, Hashtable<Integer, Double>>();
		Hashtable<Integer, Hashtable<Integer, Double>> W23 = new Hashtable<Integer, Hashtable<Integer, Double>>();

		for (Entry<Integer, WebSite> en : webSiteSet.entrySet()) {
			int id = en.getKey();
			List<Double> listProbability = en.getValue().getProbability();
			v1f.put(id, listProbability);
			
		}
		for (Entry<Integer, WebObject> en : webObjectSet.entrySet()) {
			int id = en.getKey();
			List<Double> listProbability = en.getValue().getProbability();
			v2f.put(id, listProbability);
			
		}
		for (Entry<Integer, WebObject> en : webObjectSet.entrySet()) {
			int id = en.getKey();
			int lable = en.getValue().getLable();
			trueV2ClassDic.put(id, lable);
		}
		for (Entry<Integer, WebSite> en : webSiteSet.entrySet()) {
			int id = en.getKey();
			Hashtable<Integer, Double> tagSetTemp = en.getValue().getTagList();
			W13.put(id, tagSetTemp);
		}
		for (Entry<Integer, WebObject> en : webObjectSet.entrySet()) {
			int id = en.getKey();
			Hashtable<Integer, Double> tagSetTemp = en.getValue().getTagList();
			W23.put(id, tagSetTemp);
		}

		// -----------------------------------------------------------
		// v1Count��v2Count��v3Count����������S����T��������Tag������
		// w3Dic����������tag

		int v1Count = W13.size();
		int v2Count = W23.size();
		Hashtable<Integer, Boolean> w3Dic = new Hashtable<Integer, Boolean>();
		for (Hashtable<Integer, Double> de : W13.values()) {
			for (int v3 : de.keySet()) {
				if (!w3Dic.containsKey(v3)) {
					w3Dic.put(v3, true);
				}
			}
		}
		for (Hashtable<Integer, Double> de : W23.values()) {
			for (int v3 : de.keySet()) {
				if (!w3Dic.containsKey(v3)) {
					w3Dic.put(v3, true);
				}
			}
		}
		int v3Count = w3Dic.size();
		System.out.println("v1Count: " + v1Count);
		System.out.println("v2Count: " + v2Count);
		System.out.println("v3Count: " + v3Count);
		// ------------------------------------------------------------

		Hashtable<Integer, List<Double>> v3f = new Hashtable<Integer, List<Double>>();
		for (int i = 0; i < v3Count; i++) {
			List<Double> fList = new ArrayList<Double>();
			for (int j = 0; j < 8; j++) {
				fList.add(1.0 / 8);
			}
			v3f.put(i, fList);
		}
		// -----------------------------------------------------------------
		// sumW13 S������object����������tag
		// sumW23 T������object����������tag
		// sumW31 ��S��������tag����������
		// sumW32 ��T��������tag����������

		// u belongs to V1
		Hashtable<Integer, Double> sumW13 = new Hashtable<Integer, Double>();
		for (Entry<Integer, Hashtable<Integer, Double>> en : W13.entrySet()) {
			int v1 = en.getKey();
			Hashtable<Integer, Double> v3Dic = en.getValue();
			double sum = 0;
			for (double value : v3Dic.values()) {
				sum += value;
			}
			sumW13.put(v1, sum);
		}
		// u belongs to V2
		Hashtable<Integer, Double> sumW23 = new Hashtable<Integer, Double>();
		for (Entry<Integer, Hashtable<Integer, Double>> en : W23.entrySet()) {
			int v2 = en.getKey();
			Hashtable<Integer, Double> v3Dic = en.getValue();
			double sum = 0;
			for (double value : v3Dic.values()) {
				sum += value;
			}
			sumW23.put(v2, sum);
		}

		// u belongs to V3
		Hashtable<Integer, Double> sumW31 = new Hashtable<Integer, Double>();
		Hashtable<Integer, Double> sumW32 = new Hashtable<Integer, Double>();
		for (Entry<Integer, Hashtable<Integer, Double>> en : W13.entrySet()) {
			for (Entry<Integer, Double> inde : en.getValue().entrySet()) {
				int v3 = inde.getKey();
				double value = inde.getValue();
				if (sumW31.containsKey(v3)) {
					double temp = sumW31.get(v3);
					sumW31.put(v3, temp + value);
				} else {
					sumW31.put(v3, value);
				}
			}
		}
		for (Entry<Integer, Hashtable<Integer, Double>> en : W23.entrySet()) {
			for (Entry<Integer, Double> inde : en.getValue().entrySet()) {
				int v3 = inde.getKey();
				double value = inde.getValue();
				if (sumW32.containsKey(v3)) {
					double temp = sumW32.get(v3);
					sumW32.put(v3, temp + value);
				} else {
					sumW32.put(v3, value);
				}
			}
		}

		int lineNum = 0;
		String path = "input/input0/part" + lineNum;
		
		try {
			FileWriter fw0 = new FileWriter(path, true);
			PrintWriter pw0 = new PrintWriter(fw0);
			for (int i = 0; i < v1Count; i++) {
				for (int n = 0; n < 8; n++) {
					pw0.println("v1f" + "#" + i + "#" + n + "#"
							+ v1f.get(i).get(n));
				}
			}
			pw0.close();
			fw0.close();
			lineNum++;
			path = "input/input0/part" + lineNum;
			FileWriter fw1 = new FileWriter(path, true);
			PrintWriter pw1 = new PrintWriter(fw1);
			for (int i = 0; i < v2Count; i++) {
				for (int n = 0; n < 8; n++) {
					pw1.println("v2f" + "#" + i + "#" + n + "#"
							+ v2f.get(i).get(n));
				}
			}
			pw1.close();
			fw1.close();
			lineNum++;
			path = "input/input0/part" + lineNum;
			FileWriter fw2 = new FileWriter(path, true);
			PrintWriter pw2 = new PrintWriter(fw2);
			for (int i = 0; i < v3Count; i++) {
				for (int n = 0; n < 8; n++) {
					pw2.println("v3f" + "#" + i + "#" + n + "#"
							+ v3f.get(i).get(n));
				}
			}
			pw2.close();
			fw2.close();
			lineNum++;
			path = "input/input0/part" + lineNum;
			FileWriter fw3 = new FileWriter(path, true);
			PrintWriter pw3 = new PrintWriter(fw3);
			for (Entry<Integer, Hashtable<Integer, Double>> en : W13.entrySet()) {
				int id = en.getKey();
				for (Entry<Integer, Double> inde : en.getValue().entrySet()) {
					int tagId = inde.getKey();
					double value = inde.getValue();
					pw3.println("W13" + "#" + id + "#" + tagId + "#" + value);
				}
			}
			pw3.close();
			fw3.close();
			lineNum++;
			path = "input/input0/part" + lineNum;
			FileWriter fw4 = new FileWriter(path, true);
			PrintWriter pw4 = new PrintWriter(fw4);
			for (Entry<Integer, Hashtable<Integer, Double>> en : W23.entrySet()) {
				int id = en.getKey();
				for (Entry<Integer, Double> inde : en.getValue().entrySet()) {
					int tagId = inde.getKey();
					if(id == 9 && tagId == 252){
						System.out.println("here");
					}
					double value = inde.getValue();
					pw4.println("W23" + "#" + id + "#" + tagId + "#" + value);
				}
			}
			pw4.close();
			fw4.close();
			lineNum++;
			path = "input/input0/part" + lineNum;
			FileWriter fw5 = new FileWriter(path, true);
			PrintWriter pw5 = new PrintWriter(fw5);
			for(Entry<Integer, Double> en : sumW13.entrySet()){
				int id = en.getKey();
				double value = en.getValue();
				pw5.println("sumW13" + "#" + id + "#" + value);
			}
			pw5.close();
			fw5.close();
			lineNum++;
			path = "input/input0/part" + lineNum;
			FileWriter fw6 = new FileWriter(path, true);
			PrintWriter pw6 = new PrintWriter(fw6);
			for(Entry<Integer, Double> en : sumW23.entrySet()){
				int id = en.getKey();
				double value = en.getValue();
				pw6.println("sumW23" + "#" + id + "#" + value);
			}
			pw6.close();
			fw6.close();
			lineNum++;
			path = "input/input0/part" + lineNum;
			FileWriter fw7 = new FileWriter(path, true);
			PrintWriter pw7 = new PrintWriter(fw7);
			for(Entry<Integer, Double> en : sumW31.entrySet()){
				int id = en.getKey();
				double value = en.getValue();
				pw7.println("sumW31" + "#" + id + "#" + value);
			}
			pw7.close();
			fw7.close();
			lineNum++;
			path = "input/input0/part" + lineNum;
			FileWriter fw8 = new FileWriter(path, true);
			PrintWriter pw8 = new PrintWriter(fw8);
			for(Entry<Integer, Double> en : sumW32.entrySet()){
				int id = en.getKey();
				double value = en.getValue();
				pw8.println("sumW32" + "#" + id + "#" + value);
			}
			pw8.close();
			fw8.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
