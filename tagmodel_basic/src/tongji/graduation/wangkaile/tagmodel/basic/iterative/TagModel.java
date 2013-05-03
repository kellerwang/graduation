package tongji.graduation.wangkaile.tagmodel.basic.iterative;

import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.List;
import java.util.Map.Entry;

// hashmap 在不要求线程同步时效率更高
public class TagModel {
	// The function is the core of iterative algorithm
	// There are three types of vertices. V1: type S, V2: type T, V3: Tag
	// categoryCount is the category size
	// iteration is the upper limit of iterations
	// alpha, beta, gamma are three paramters in the objective function
	// fixedv1f is the initial class distributions for V1 (V_S)
	// fixedv2f is the initial class distributions for labeled V2 (V_T^l)
	// priorv2f is the prior class distributions for unlabeled V2 (V_T^u)
	// trueV2ClassDic is the dictionary to store the ground truth of V2
	// W13 are the weights for (u,v) u \in V_S, v \in Tag
	// W23 are the weights for (u,v) u \in V_T, v \in Tag
	// accuracyDiffThresh is the termination condition for accuracy change
	// v1f is the output class distributions for V1
	// v2f is the output class distributions for V2 (which leads to the final
	// classification result of V_T^u)
	// v3f is the output class distributions for V3
	private static Hashtable<Integer, List<Double>> v1f = new Hashtable<Integer, List<Double>>();
	private static Hashtable<Integer, List<Double>> v2f = new Hashtable<Integer, List<Double>>();
	private static Hashtable<Integer, List<Double>> v3f = new Hashtable<Integer, List<Double>>();

	public static void Iterate(int categoryCount, int iteration, double alpha,
			double beta, double gamma,
			Hashtable<Integer, List<Double>> fixedv1f,
			Hashtable<Integer, List<Double>> fixedv2f,
			Hashtable<Integer, List<Double>> priorv2f,
			Hashtable<Integer, Integer> trueV2ClassDic,
			Hashtable<Integer, Hashtable<Integer, Double>> W13,
			Hashtable<Integer, Hashtable<Integer, Double>> W23,
			double accuracyDiffThresh) {

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

		// ---------------------------------------------------------
		// 初始化输出结果 oldAccuracy是正确率的基准值
		double oldAccuracy = 0;
		Hashtable<Integer, List<Double>> v1f2, v2f2, v3f2;
		v1f = new Hashtable<Integer, List<Double>>();
		v2f = new Hashtable<Integer, List<Double>>();
		v3f = new Hashtable<Integer, List<Double>>();
		v1f2 = new Hashtable<Integer, List<Double>>();
		v2f2 = new Hashtable<Integer, List<Double>>();
		v3f2 = new Hashtable<Integer, List<Double>>();

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
		// 把所有输出的初始值赋值为1/categoryCount
		for (int i = 0; i < v1Count; i++) {
			List<Double> fList = new ArrayList<Double>();
			for (int j = 0; j < categoryCount; j++) {
				fList.add(1.0 / categoryCount);
			}
			v1f.put(i, fList);
		}
		for (int i = 0; i < v2Count; i++) {
			List<Double> fList = new ArrayList<Double>();
			for (int j = 0; j < categoryCount; j++) {
				fList.add(1.0 / categoryCount);
			}
			v2f.put(i, fList);
		}
		for (int i = 0; i < v3Count; i++) {
			List<Double> fList = new ArrayList<Double>();
			for (int j = 0; j < categoryCount; j++) {
				fList.add(1.0 / categoryCount);
			}
			v3f.put(i, fList);
		}
		// --------------------------------------------------------------
		// 迭代算法开始
		for (int i = 0; i < iteration; i++) {
			System.out.println("Iteration " + (i + 1));
			// ----------------------------------------------------------
			// u belongs to v1
			for (int j = 0; j < v1Count; j++) {
				List<Double> newf = new ArrayList<Double>();
				for (int n = 0; n < categoryCount; n++)
					newf.add(0.0);
				double denominator = alpha + sumW13.get(j);
				// alpha无限大，TypeS的输入值作为TypeS的输出值
				if (alpha == Double.MAX_VALUE) {
					for (int n = 0; n < categoryCount; n++)
						newf.set(n, fixedv1f.get(j).get(n));
				}
				// alpha为0时TypeS类不存在
				else if (alpha != 0) {
					for (int n = 0; n < categoryCount; n++) {
						double temp = newf.get(n) + (alpha / denominator)
								* fixedv1f.get(j).get(n);
						newf.set(n, temp);
					}
					for (int k = 0; k < v3Count; k++) {
						if (W13.get(j).containsKey(k) && v3f.containsKey(k)) {
							for (int n = 0; n < categoryCount; n++) {
								double temp = newf.get(n)
										+ (W13.get(j).get(k)
												* v3f.get(k).get(n) / denominator);
								newf.set(n, temp);
							}
						}
					}
				}
				v1f2.put(j, newf);
			}
			// -----------------------------------------------------------
			// u belongs to v2
			for (int j = 0; j < v2Count; j++) {
				List<Double> newf = new ArrayList<Double>();
				for (int n = 0; n < categoryCount; n++)
					newf.add(0.0);

				if (fixedv2f.containsKey(j)) {
					if (beta == Double.MAX_VALUE) {
						for (int n = 0; n < categoryCount; n++)
							newf.set(n, fixedv2f.get(j).get(n));
					} else {
						double denominator = beta + sumW23.get(j);
						for (int n = 0; n < categoryCount; n++) {
							double temp = newf.get(n)
									+ ((beta / denominator) * fixedv2f.get(j)
											.get(n));
							newf.set(n, temp);
						}
						for (int k = 0; k < v3Count; k++) {
							if (W23.get(j).containsKey(k) && v3f.containsKey(k)) {
								for (int n = 0; n < categoryCount; n++) {
									double temp = newf.get(n)
											+ (W23.get(j).get(k)
													* v3f.get(k).get(n) / denominator);
									newf.set(n, temp);
								}
							}
						}
					}
				} else if (priorv2f.containsKey(j)) {
					double denominator = gamma + sumW23.get(j);
					if (denominator != 0) {
						for (int n = 0; n < categoryCount; n++) {
							double temp = newf.get(n) + (gamma / denominator)
									* priorv2f.get(j).get(n);
							newf.set(n, temp);
						}
						for (int k = 0; k < v3Count; k++) {
							if (W23.get(j).containsKey(k) && v3f.containsKey(k)) {
								for (int n = 0; n < categoryCount; n++) {
									double temp = newf.get(n)
											+ (W23.get(j).get(k)
													* v3f.get(k).get(n) / denominator);
									newf.set(n, temp);
								}
							}
						}
					} else {
//						System.out.println("denominator == 0!");
					}
				}
				// 这些对象不需要分类，虽然他们也在TypeT类中而且不知道类别
				else {
					double denominator = sumW23.get(j);
					for (int k = 0; k < v3Count; k++) {
						if (W23.get(j).containsKey(k) && v3f.containsKey(k)) {
							for (int n = 0; n < categoryCount; n++) {
								double temp = newf.get(n) + (1.0 / denominator)
										* W23.get(j).get(k) * v3f.get(k).get(n);
								newf.set(n, temp);
							}
						}
					}
				}
				v2f2.put(j, newf);
			}
			// ----------------------------------------------------------------------
			// u belongs to v3
			for (int j = 0; j < v3Count; j++) {
				if (!sumW31.containsKey(j) || !sumW32.containsKey(j))
					continue;
				List<Double> newf = new ArrayList<Double>();
				for (int n = 0; n < categoryCount; n++)
					newf.add(0.0);

				if (alpha != 0) {
					double denominator = sumW31.get(j) + sumW32.get(j);
					if (denominator != 0) {
						for (int k1 = 0; k1 < v1Count; k1++) {
							if (W13.get(k1).containsKey(j)) {
								for (int n = 0; n < categoryCount; n++) {
									double temp = newf.get(n)
											+ W13.get(k1).get(j)
											* v1f.get(k1).get(n) / denominator;
									newf.set(n, temp);
								}
							}
						}
						for (int k2 = 0; k2 < v2Count; k2++) {
							if (W23.get(k2).containsKey(j)) {
								for (int n = 0; n < categoryCount; n++) {
									double temp = newf.get(n)
											+ W23.get(k2).get(j)
											* v2f.get(k2).get(n) / denominator;
									newf.set(n, temp);
								}
							}
						}
					} else {
//						System.out.println("denominator == 0!");
					}
				} else {
					double denominator = sumW32.get(j);
					if (denominator != 0) {
						for (int k2 = 0; k2 < v2Count; k2++) {
							if (W23.get(k2).containsKey(j)) {
								for (int n = 0; n < categoryCount; n++) {
									double temp = newf.get(n)
											+ W23.get(k2).get(j)
											* v2f.get(k2).get(n) / denominator;
									newf.set(n, temp);
								}
							}
						}
					} else {
//						System.out.println("denominator == 0!");
					}
				}
				v3f2.put(j, newf);
			}
			// ----------------------------------------------------------------------
			// 看每次计算的正确率的差值
			double accuracy = 0;
			double accuracyDiff = 0;
			if (i > 0) {
				accuracy = processResult(v2f, fixedv2f, trueV2ClassDic,
						categoryCount);
				accuracyDiff = Math.abs(accuracy - oldAccuracy);
				oldAccuracy = accuracy;
				System.out.println("Accuracy Diff: " + accuracyDiff);
				// 取消了迭代次数的限定
				if (accuracyDiff < accuracyDiffThresh && i >= 10)
					break;

			}
			// ------------------------------------------------------------------------
			// 一次为结果赋值
			v1f = clone(v1f2);
			v2f = clone(v2f2);
			v3f = clone(v3f2);

			// ------------------------------------------------------------------------
			// 重新赋值
			v1f2 = new Hashtable<Integer, List<Double>>();
			v2f2 = new Hashtable<Integer, List<Double>>();
			v3f2 = new Hashtable<Integer, List<Double>>();
		}
	}

	// 克隆方法
	private static Hashtable<Integer, List<Double>> clone(
			Hashtable<Integer, List<Double>> v2f) {
		Hashtable<Integer, List<Double>> retDic = new Hashtable<Integer, List<Double>>();
		for (Entry<Integer, List<Double>> de : v2f.entrySet()) {
			int v = de.getKey();
			List<Double> fList = new ArrayList<Double>();
			for (double f : de.getValue()) {
				fList.add(f);
			}
			retDic.put(v, fList);
		}
		return retDic;
	}

	//
	private static double processResult(Hashtable<Integer, List<Double>> v2f,
			Hashtable<Integer, List<Double>> fixedv2f,
			Hashtable<Integer, Integer> trueV2ClassDic, int categoryCount) {
		int correct = 0;
		int totalCount = v2f.size() - fixedv2f.size();

		Hashtable<Integer, Integer> resultDic = new Hashtable<Integer, Integer>();
		List<Double> priorList = new ArrayList<Double>();
		for (int i = 0; i < categoryCount; i++) {
			priorList.add(0.0);
		}
		for (List<Double> de : v2f.values()) {
			for (int i = 0; i < categoryCount; i++) {
				double temp = priorList.get(i) + de.get(i);
				priorList.set(i, temp);
			}
		}
		for (Entry<Integer, List<Double>> de : v2f.entrySet()) {
			int docid = de.getKey();
			int classid = trueV2ClassDic.get(docid);
			if (fixedv2f.containsKey(docid))
				continue;
			List<Double> fList = new ArrayList<Double>(de.getValue());
			for (int j = 0; j < categoryCount; j++) {
				fList.set(j, (fList.get(j) / priorList.get(j)));
			}
			int label = getLabel(fList);
			resultDic.put(docid, label);
			if (classid == label)
				correct++;
		}

		Hashtable<Integer, Integer> resultClassCountDic = new Hashtable<Integer, Integer>();
		Hashtable<Integer, Integer> truthClassCountDic = new Hashtable<Integer, Integer>();

		for (Entry<Integer, Integer> de : trueV2ClassDic.entrySet()) {
			int docid = de.getKey();
			int classid = de.getValue();
			if (fixedv2f.containsKey(docid))
				continue;
			if (truthClassCountDic.containsKey(classid))
				truthClassCountDic.put(classid,
						truthClassCountDic.get(classid) + 1);
			else
				truthClassCountDic.put(classid, 1);
		}

		for (int i = 0; i < categoryCount; i++)
			resultClassCountDic.put(i, 0);
		for (int classid : resultDic.values()) {
			resultClassCountDic.put(classid,
					resultClassCountDic.get(classid) + 1);
		}

		int classCount = resultClassCountDic.size();

		List<Double> pList = new ArrayList<Double>();
		List<Double> rList = new ArrayList<Double>();

		List<Integer> tpList = new ArrayList<Integer>();

		for (int i = 0; i < classCount; i++) {
			// 每个类正确的次数
			int tp = 0;
			for (Entry<Integer, Integer> de : resultDic.entrySet()) {
				int id = de.getKey();
				int classid = de.getValue();
				int truthclassid = trueV2ClassDic.get(id);
				if (classid == truthclassid && classid == i)
					tp++;
			}
			pList.add((double) tp / resultClassCountDic.get(i));
			rList.add((double) tp / truthClassCountDic.get(i));
		}

		double F1 = 0;
		for (int i = 0; i < pList.size(); i++) {
			double p = pList.get(i);
			double r = rList.get(i);
			F1 += 2 * p * r / (p + r);
		}
		F1 = F1 / classCount;

		String outputLine = "Prior : true\nAccuracy: " + correct + "/"
				+ totalCount + "=" + (correct / (double) totalCount) + "\n"
				+ "MacroF1: " + F1;
		System.out.println(outputLine);

		return (correct / (double) totalCount);
	}

	//
	private static int getLabel(List<Double> fList) {
		double maxf = -1;
		int maxlabel = 0;
		for (int i = 0; i < fList.size(); i++) {
			if (fList.get(i) > maxf) {
				maxlabel = i;
				maxf = fList.get(i);
			}
		}
		return maxlabel;
	}
}

