package com.iteritory.ignite.ignite_tutorial;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;

public class FirstProgram {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
  // Set the node start mode as client; so, this node will join the apache
		//cluster as client
		
		Ignition.setClientMode(true);
		
		// Here we provide the cache configuration file
		
		Ignite objIgnite = Ignition.start("G:\\apache-ignite-fabric-2.6.0-bin\\apache-ignite-fabric-2.6.0-bin\\config\\itc-poc-config.xml");
	
		IgniteCache<Integer, String> objIgniteCache = objIgnite.getOrCreateCache("myFirstIgniteCache");
	 
		// Populating cache with few values
		objIgniteCache.put(1, "Salman");
		objIgniteCache.put(2, "Abhishek");
		objIgniteCache.put(3, "Krishna");
		objIgniteCache.put(4, "Jay");
		
		// Get these items from cache
		
		System.out.println(objIgniteCache.get(1));
		System.out.println(objIgniteCache.get(2));
		System.out.println(objIgniteCache.get(3));
		System.out.println(objIgniteCache.get(4));
	}

}
