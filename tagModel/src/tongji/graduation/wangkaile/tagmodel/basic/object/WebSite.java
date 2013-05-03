package tongji.graduation.wangkaile.tagmodel.basic.object;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;

public class WebSite {
	private String description;
	private Hashtable<Integer, Double> tagList = new Hashtable<Integer, Double>();
	private int lable;
	private List<Double> probability = new ArrayList<Double>();
	
	public WebSite(int lable){
		this.lable = lable;
	}
	
	public void initializeProbability(){
		for(int i = 0; i < 8; i++){
			if(i == lable){
				probability.add(i, 1.0);
			}
			else{
				probability.add(i, 0.0);
			}
		}
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public Hashtable<Integer, Double> getTagList() {
		return tagList;
	}

	public int getLable() {
		return lable;
	}

	public void setLable(int lable) {
		this.lable = lable;
	}

	public List<Double> getProbability() {
		return probability;
	}

	public void setProbability(List<Double> probability) {
		this.probability = probability;
	}

}
