package tongji.graduation.wangkaile.tagmodel.basic.object;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;

public class WebObject {
	private String realId;
	private String description;
	private Hashtable<Integer, Double> tagList = new Hashtable<Integer, Double>();
	private int lable = -1;
	private List<Double> probability = new ArrayList<Double>();

	public WebObject(int lable){
		this.lable= lable;
	}
	
	public void initializeProbability4LableObject(){
		for(int i = 0; i < 8; i++){
			if(i == lable){
				probability.add(i, 1.0);
			}
			else{
				probability.add(i, 0.0);
			}
		}
	}
	public void initializeProbability4UnlableObject(){
		for(int i = 0; i < 8; i++){
			probability.add(i, 1.0 / 8);
		}
	}
	
	public String getRealId() {
		return realId;
	}

	public void setRealId(String realId) {
		this.realId = realId;
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

}
