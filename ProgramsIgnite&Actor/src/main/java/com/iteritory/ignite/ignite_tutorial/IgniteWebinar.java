package com.iteritory.ignite.ignite_tutorial;

import org.apache.ignite.*; 
import org.apache.ignite.internal.*;

public class IgniteWebinar {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
	//  Ignition.setClientMode(true);
		try (Ignite ignite = Ignition.start("G:\\apache-ignite-fabric-2.6.0-bin\\apache-ignite-fabric-2.6.0-bin\\examples\\config\\example-ignite.xml")){
	     
			IgniteCompute  compute = ignite.compute();
			
			compute.broadcast (() -> System.out.println("Hello node"));
		}
		catch(Exception e) {
	}
	}

}
