package com.trite.apps;

public class Human {
	private boolean _isAlive;
	private String _name;
	private long _lifeSpan;
	private double _height;
	private double _weight;
	
	public boolean isAlive() {
		return _isAlive;
	}
	public void Live(boolean _isAlive) {
		this._isAlive = _isAlive;
	}
	public String getName() {
		return _name;
	}
	public void setName(String _name) {
		this._name = _name;
	}
	public long getLifeSpan() {
		return _lifeSpan;
	}
	public void setLifeSpan(long _lifeSpan) {
		this._lifeSpan = _lifeSpan;
	}
	public double getHeight() {
		return _height;
	}
	public void setHeight(double _height) {
		this._height = _height;
	}
	public double getWeight() {
		return _weight;
	}
	public void setWeight(double _weight) {
		this._weight = _weight;
	}
	
	public Human(boolean _isAlive, String _name, long _lifeSpan,
			double _height, double _weight) {
		super();
		this._isAlive = _isAlive;
		this._name = _name;
		this._lifeSpan = _lifeSpan;
		this._height = _height;
		this._weight = _weight;
	}
	
	public Human() {
		// TODO Auto-generated constructor stub
	}
	

	public void Sleep()
	{
		try {
			Thread.sleep(3000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void Eat()
	{
		
	}
	
	public void Work()
	{
		
	}
	
	public void Play()
	{
		
	}
}
