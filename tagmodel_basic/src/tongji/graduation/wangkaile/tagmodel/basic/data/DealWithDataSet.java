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

		try {
			reader = new BufferedReader(new FileReader(file));
			String tempString = null;
			int flag = 0;
			// 临时存储亚马逊商品对象信息
			WebObject tempObject = new WebObject(lable);
			tempObject.initializeProbability4UnlableObject();
			while ((tempString = reader.readLine()) != null) {
				// 读到空行，一个数据块写入完成
				if (tempString.trim().equals("") && flag != 1) {
					flag = 0;
					webObjectSet.put(index, tempObject);
					tempObject = new WebObject(lable);
					tempObject.initializeProbability4UnlableObject();
					index++;
					continue;
				}
				// 现在开始读入社会标签
				if (flag == 2) {
					String[] strArray = tempString.split("	");
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
				// 开始读一个新的数据块
				if (flag == 0 || flag == 1) {
					switch (flag) {
					case 0:
						tempObject.setRealId(tempString);
						break;
					case 1:
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
			reader = new BufferedReader(new FileReader(file));
			String tempString = null;
			int flag = 0;
			// 存储网站对象的临时变量
			WebSite tempSite = new WebSite(lable);
			tempSite.initializeProbability();
			while ((tempString = reader.readLine()) != null) {
				// 读到空行，一个数据块处理结束
				if (tempString.trim().equals("")) {
					flag = 0;
					webSiteSet.put(indexWebSite, tempSite);
					tempSite = new WebSite(lable);
					tempSite.initializeProbability();
					indexWebSite++;
					continue;
				}
				// 开始处理社会标签
				if (flag == 1) {
					String[] strArray = tempString.split("	");
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
				// 一个新的数据块开始
				if (flag == 0) {
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
		for (int dataSize = 0; dataSize < 6; dataSize++) {
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
		}

		System.out.println("Input Over");
		TagModel tagmodel = new TagModel();
		Hashtable<Integer, List<Double>> fixedv1f = new Hashtable<Integer, List<Double>>();
		Hashtable<Integer, List<Double>> fixedv2f = new Hashtable<Integer, List<Double>>();
		Hashtable<Integer, List<Double>> priorv2f = new Hashtable<Integer, List<Double>>();
		Hashtable<Integer, Integer> trueV2ClassDic = new Hashtable<Integer, Integer>();
		Hashtable<Integer, Hashtable<Integer, Double>> W13 = new Hashtable<Integer, Hashtable<Integer, Double>>();
		Hashtable<Integer, Hashtable<Integer, Double>> W23 = new Hashtable<Integer, Hashtable<Integer, Double>>();
		double accuracyDiffThresh;
		for (Entry<Integer, WebSite> en : webSiteSet.entrySet()) {
			int id = en.getKey();
			List<Double> listProbability = en.getValue().getProbability();
			fixedv1f.put(id, listProbability);
		}
		for (Entry<Integer, WebObject> en : webObjectSet.entrySet()) {
			int id = en.getKey();
			List<Double> listProbability = en.getValue().getProbability();
			priorv2f.put(id, listProbability);
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
		tagmodel.Iterate(8, 10, Double.MAX_VALUE, 0, 0, fixedv1f, fixedv2f,
				priorv2f, trueV2ClassDic, W13, W23, 0.01);
	}

}
