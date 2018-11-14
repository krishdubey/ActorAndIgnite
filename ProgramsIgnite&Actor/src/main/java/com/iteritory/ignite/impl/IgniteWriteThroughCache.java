package com.iteritory.ignite.impl;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.transactions.Transaction;
 
import com.iteritory.ignite.adapter.OrdersCacheStoreAdapter;
import com.iteritory.ignite.model.OrderLines;
import com.iteritory.ignite.model.Orders;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
 
import java.text.SimpleDateFormat;
 
import javax.cache.configuration.FactoryBuilder;

public class IgniteWriteThroughCache {

	/** Orders cache name. */
	private static final String ORDERS_CACHE = "Orders_Cache";
 
	public static void main(String[] args) {
		try {
			// Set the node start mode as client; so, this node will join the
			// apache
			// cluster as client
			Ignition.setClientMode(true);
 
			// Here, we provide the cache configuration file
			Ignite objIgnite = Ignition.start("G:\\apache-ignite-fabric-2.6.0-bin\\apache-ignite-fabric-2.6.0-bin\\config\\itc-poc-config.xml");
			CacheConfiguration<Long, Orders> ordersCacheCfg = new CacheConfiguration<Long, Orders>(ORDERS_CACHE);
 
			// Set atomicity as transaction, since we are showing transactions
			// in
			// example.
			ordersCacheCfg.setAtomicityMode(TRANSACTIONAL);
 
			// Configure JDBC store.
			ordersCacheCfg.setCacheStoreFactory(FactoryBuilder.factoryOf(OrdersCacheStoreAdapter.class));
 
			// Set as write-thorugh cache
			ordersCacheCfg.setWriteThrough(true);
 
			IgniteCache<Long, Orders> cache = objIgnite.getOrCreateCache(ordersCacheCfg);
			// Start transaction and execute several cache operations with
			// read/write-through to persistent store.
			persistData(cache);
		} catch (Exception ex) {
			ex.printStackTrace();
		}
 
	}
 
	private static void persistData(IgniteCache<Long, Orders> cache) {
		try {
 
			Transaction tx = Ignition.ignite().transactions().txStart();
			System.out.println("START:Transaction started");
			// Put/Insert first order
			OrderLines olBanana = new OrderLines(11, 1, "Banana", 12);
			OrderLines olApple = new OrderLines(11, 2, "Apple", 6);
			OrderLines[] ol1 = new OrderLines[] { olBanana, olApple };
 
			java.util.Date utilDate1 = new SimpleDateFormat("dd-MM-yyyy").parse("01-01-2017");
			java.sql.Date sqlDate1 = new java.sql.Date(utilDate1.getTime());
			Orders ord1 = new Orders(11, "EcomDeliveryOrder", sqlDate1, ol1);
			cache.put((long) 11, ord1);
 
			// Put/Insert second order
			OrderLines olMosambi = new OrderLines(22, 1, "Mosambi", 3);
			OrderLines olMango = new OrderLines(22, 2, "Mango", 4);
			OrderLines[] ol2 = new OrderLines[] { olMosambi, olMango };
			java.util.Date utilDate2 = new SimpleDateFormat("dd-MM-yyyy").parse("02-01-2017");
			java.sql.Date sqlDate2 = new java.sql.Date(utilDate2.getTime());
			Orders ord2 = new Orders(22, "StorePickupOrder", sqlDate2, ol2);
			cache.put((long) 22, ord2);
 
			tx.commit();
			
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}
}
