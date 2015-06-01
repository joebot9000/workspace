package com.trite.apps;

public class Me {

	private static long endDayTime;
	private static long startDayTime;
	private static long currentLifeSpan;
	private static long totalLifeSpan;
	private static double height;
	private static double weight;
	
	public static void main(String[] args) {
//		totalLifeSpan = Long.parseLong(args[0]);
//		height = Double.parseDouble(args[1]);
//		weight = Long.parseLong(args[2]);
		
		totalLifeSpan = 220752000;
		height = 72.5;
		weight = 230.5;
		
		Human joe = new Human();		
		joe.setName("Joe");
		joe.setWeight(weight);
		joe.setHeight(height);	
		joe.setLifeSpan(totalLifeSpan);
		joe.Live(true);

		while (joe.isAlive())
		{
			startDayTime = System.currentTimeMillis();
			
			joe.Eat();
			joe.Work();
			joe.Play();
			joe.Sleep();

			endDayTime = System.currentTimeMillis();
			
			currentLifeSpan += (currentLifeSpan + (endDayTime - startDayTime));
			
			if(currentLifeSpan >= totalLifeSpan)
			{
				joe.Live(false);
				System.out.println(joe.getName() + " is dead at the age of " + currentLifeSpan + " seconds.");
			}
			else
			{
				System.out.println(joe.getName() + " is still alive and is " + currentLifeSpan + " seconds old.");
			}
		}
	}

}
