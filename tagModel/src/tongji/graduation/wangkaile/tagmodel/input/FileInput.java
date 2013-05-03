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
	// 存储最终webObject数据
	private static Hashtable<Integer, WebObject> webObjectSet;
	private static Hashtable<String, Integer> tagSet;
	private static Hashtable<Integer, WebSite> webSiteSet;
	// 这个index就是后面TagModel中webObject的id
	private static int index = 0;
	private static int indexWebSite = 0;
	// 这个indexTag就是后面TagModel中tag的id
	private static int indexTag = 0;

	public static void readFileByLines4Amazon(String fileName, int lable) {
		File file = new File(fileName);
		BufferedReader reader = null;

		try {
			// System.out.println("以行为单位读取文件内容，一次读一整行：");
			reader = new BufferedReader(new FileReader(file));
			String tempString = null;
			int flag = 0;
			// 一次读入一行，直到读入null为文件结束
			WebObject tempObject = new WebObject(lable);
			tempObject.initializeProbability4UnlableObject();
			while ((tempString = reader.readLine()) != null) {
				// 读到空行，即马上处理下一对象。description可以是空行！
				if (tempString.trim().equals("") && flag != 1) {
					flag = 0;
					webObjectSet.put(index, tempObject);
					tempObject = new WebObject(lable);
					tempObject.initializeProbability4UnlableObject();
					index++;
					System.out.println("空行");
					continue;
				}
				// 此时输入行为tags
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
				// 如果是数据id和描述
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

		try {
			// System.out.println("以行为单位读取文件内容，一次读一整行：");
			reader = new BufferedReader(new FileReader(file));
			String tempString = null;
			int flag = 0;
			// 一次读入一行，直到读入null为文件结束
			WebSite tempSite = new WebSite(lable);
			tempSite.initializeProbability();
			while ((tempString = reader.readLine()) != null) {
				// 读到空行，即马上处理下一对象。description可以是空行！
				if (tempString.trim().equals("")) {
					flag = 0;
					webSiteSet.put(indexWebSite, tempSite);
					tempSite = new WebSite(lable);
					tempSite.initializeProbability();
					indexWebSite++;
					System.out.println("空行");
					continue;
				}
				// 此时输入行为tags
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
				// 如果是数据描述
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
		// Amazon数据输入
		readFileByLines4Amazon("data/amazon/Books.txt", 0);
		readFileByLines4Amazon("data/amazon/Electronics.txt", 1);
		readFileByLines4Amazon("data/amazon/HealthPersonCare.txt", 2);
		readFileByLines4Amazon("data/amazon/HomeGarden.txt", 3);
		readFileByLines4Amazon("data/amazon/Jewelry.txt", 4);
		readFileByLines4Amazon("data/amazon/Music.txt", 5);
		readFileByLines4Amazon("data/amazon/OfficeProducts.txt", 6);
		readFileByLines4Amazon("data/amazon/PetSupplies.txt", 7);
		// odp数据输入
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
		// v1Count，v2Count，v3Count分别记录了S类，T类对象，Tag的数量
		// w3Dic记录了所以tag

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
		// sumW13 S类每个object含有多少个tag
		// sumW23 T类每个object含有多少个tag
		// sumW31 和S类关联的tag出现的频率
		// sumW32 和T类关联的tag出现的频率

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

		String path = "input/input.txt";
		FileWriter fw;
		try {
			fw = new FileWriter(path, true);
			PrintWriter pw = new PrintWriter(fw);

			for (int i = 0; i < v1Count; i++) {
				for (int n = 0; n < 8; n++) {
					pw.println("v1f" + "#" + i + "#" + n + "#"
							+ v1f.get(i).get(n));
				}
			}
			for (int i = 0; i < v2Count; i++) {
				for (int n = 0; n < 8; n++) {
					pw.println("v2f" + "#" + i + "#" + n + "#"
							+ v2f.get(i).get(n));
				}
			}
			for (int i = 0; i < v3Count; i++) {
				for (int n = 0; n < 8; n++) {
					pw.println("v3f" + "#" + i + "#" + n + "#"
							+ v3f.get(i).get(n));
				}
			}
			for (Entry<Integer, Hashtable<Integer, Double>> en : W13.entrySet()) {
				int id = en.getKey();
				for (Entry<Integer, Double> inde : en.getValue().entrySet()) {
					int tagId = inde.getKey();
					double value = inde.getValue();
					pw.println("W13" + "#" + id + "#" + tagId + "#" + value);
				}
			}
			for (Entry<Integer, Hashtable<Integer, Double>> en : W23.entrySet()) {
				int id = en.getKey();
				for (Entry<Integer, Double> inde : en.getValue().entrySet()) {
					int tagId = inde.getKey();
					double value = inde.getValue();
					pw.println("W23" + "#" + id + "#" + tagId + "#" + value);
				}
			}
			for(Entry<Integer, Double> en : sumW13.entrySet()){
				int id = en.getKey();
				double value = en.getValue();
				pw.println("sumW13" + "#" + id + "#" + value);
			}
			for(Entry<Integer, Double> en : sumW23.entrySet()){
				int id = en.getKey();
				double value = en.getValue();
				pw.println("sumW23" + "#" + id + "#" + value);
			}
			for(Entry<Integer, Double> en : sumW31.entrySet()){
				int id = en.getKey();
				double value = en.getValue();
				pw.println("sumW31" + "#" + id + "#" + value);
			}
			for(Entry<Integer, Double> en : sumW32.entrySet()){
				int id = en.getKey();
				double value = en.getValue();
				pw.println("sumW32" + "#" + id + "#" + value);
			}
			pw.close();
			fw.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
