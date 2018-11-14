package com.iteritory.ignite.ignite_tutorial;

import org.apache.ignite.*;

public class IgniteNodeStartup {

	public static void main(String[] args) throws IgniteException
	{
         Ignition.setClientMode(true);
		
		// Here we provide the cache configuration file
		
		Ignition.start("G:\\apache-ignite-fabric-2.6.0-bin\\apache-ignite-fabric-2.6.0-bin\\config\\itc-poc-config.xml");
		//Ignition.start("examples\\config\\example-default.xml");
		// TODO Auto-generated method stub

	}

}
