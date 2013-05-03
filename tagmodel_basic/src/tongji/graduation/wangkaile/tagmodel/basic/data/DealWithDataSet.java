package tongji.graduation.wangkaile.tagmodel.basic.data;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import java.util.Hashtable;
import java.util.List;
import java.util.Map.Entry;

import tongji.graduation.wangkaile.tagmodel.basic.iterative.TagModel;
import tongji.graduation.wangkaile.tagmodel.basic.object.WebObject;
import tongji.graduation.wangkaile.tagmodel.basic.object.WebSite;

public class DealWithDataSet {

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
		TagModel tagmodel = new TagModel();
		Hashtable<Integer, List<Double>> fixedv1f = new Hashtable<Integer, List<Double>>();
		Hashtable<Integer, List<Double>> fixedv2f = new Hashtable<Integer, List<Double>>();
		Hashtable<Integer, List<Double>> priorv2f = new Hashtable<Integer, List<Double>>();
		Hashtable<Integer, Integer> trueV2ClassDic = new Hashtable<Integer, Integer>();
		Hashtable<Integer, Hashtable<Integer, Double>> W13 = new Hashtable<Integer, Hashtable<Integer, Double>>();
		Hashtable<Integer, Hashtable<Integer, Double>> W23 = new Hashtable<Integer, Hashtable<Integer, Double>>();
		double accuracyDiffThresh;
		for(Entry<Integer, WebSite> en : webSiteSet.entrySet()){
			int id = en.getKey();
			List<Double> listProbability = en.getValue().getProbability();
			fixedv1f.put(id, listProbability);
		}
		for(Entry<Integer, WebObject> en : webObjectSet.entrySet()){
			int id = en.getKey();
			List<Double> listProbability = en.getValue().getProbability();
			priorv2f.put(id, listProbability);
		}
		for(Entry<Integer, WebObject> en : webObjectSet.entrySet()){
			int id = en.getKey();
			int lable = en.getValue().getLable();
			trueV2ClassDic.put(id, lable);
		}
		for(Entry<Integer, WebSite> en : webSiteSet.entrySet()){
			int id = en.getKey();
			Hashtable<Integer, Double> tagSetTemp = en.getValue().getTagList();
			W13.put(id, tagSetTemp);
		}
		for(Entry<Integer, WebObject> en : webObjectSet.entrySet()){
			int id = en.getKey();
			Hashtable<Integer, Double> tagSetTemp = en.getValue().getTagList();
			W23.put(id, tagSetTemp);
		}
		tagmodel.Iterate(8, 10, Double.MAX_VALUE, 0, 0, fixedv1f, fixedv2f, priorv2f, trueV2ClassDic, W13, W23, 0.1);
	}

}
