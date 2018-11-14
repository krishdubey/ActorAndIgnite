package com.iteritory.ignite.adapter;
 
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;
 
import javax.cache.Cache.Entry;
import javax.cache.integration.CacheLoaderException;
import javax.cache.integration.CacheWriterException;
 
import org.apache.ignite.cache.store.CacheStoreAdapter;
import org.apache.ignite.cache.store.CacheStoreSession;
import org.apache.ignite.resources.CacheStoreSessionResource;
import org.jetbrains.annotations.Nullable;
 
import com.iteritory.ignite.model.*;
 
public class OrdersCacheStoreAdapter extends CacheStoreAdapter<Long, Orders> {
 
	// This is a key to store connection
	private static final String CONN_NAME = "CONN_STORE";
 
	@CacheStoreSessionResource
	private CacheStoreSession cacheStoreSession;
 
	public Orders load(Long key) throws CacheLoaderException {
		// TODO Auto-generated method stub
		return null;
	}
 
	public void write(Entry<? extends Long, ? extends Orders> entry) throws CacheWriterException {
		// TODO Auto-generated method stub
		
		//Get the key and value for every insert call from the invokation class IgniteWtiteThroughCache
		Long key = entry.getKey();
		Orders value = entry.getValue();
 
		System.out.println("INFO: Inserting the record for order#:" + key);
		Connection conn = null;
		try {
			conn = connection();
 
			PreparedStatement stOrder, stOrderLine;
			
			// Delete the row if any from the orderlines table for the current key
			stOrder = conn.prepareStatement("delete from orders where order_number = ?");
			stOrderLine = conn.prepareStatement("delete from order_line where order_number = ?");
			stOrder.setLong(1, value.getOrderNumber());
			stOrderLine.setLong(1, value.getOrderNumber());
			stOrderLine.executeUpdate();
			stOrder.executeUpdate();
 
			// Insert the rows into table 
			stOrder = conn.prepareStatement("insert into orders (order_number, order_type, order_fulfillment_date) values (?, ?, ?)");
			stOrderLine = conn.prepareStatement("insert into order_line (order_number, order_line_number, item_name,item_qty) values (?, ?, ?,?)");
			stOrder.setLong(1, value.getOrderNumber());
			stOrder.setString(2, value.getOrderType());
			stOrder.setDate(3, value.getOrderFulfillmentDate());
			stOrder.executeUpdate();
 
			for (int i =0; i < value.getOrderLine().length; i ++){
				OrderLines currentOrderLine = value.getOrderLine()[i];
				stOrderLine.setInt(1, currentOrderLine.getOrderNumber());
				stOrderLine.setInt(2, currentOrderLine.getOrderLineNumber());
				stOrderLine.setString(3, currentOrderLine.getItemName());
				stOrderLine.setInt(4, currentOrderLine.getItemQty());
				stOrderLine.executeUpdate();
			}
		} catch (Exception ex) {
			throw new CacheLoaderException("Failed to put object [key=" + key + ", val=" + value + ']', ex);
		} finally {
			endConnection(conn);
		}
	}
 
	public void delete(Object key) throws CacheWriterException {
		// TODO Auto-generated method stub
 
	}
 
	@Override
	public void sessionEnd(boolean commit) {
		Map<String, Connection> connectionProperties = cacheStoreSession.properties();
		try {
			Connection conn = (Connection) connectionProperties.remove(CONN_NAME);
			if (conn != null) {
				//when the transaction is successfully committed [tx.commit() in IgniteWriteThroughCache],
				//the commit variable will be true
				if (commit)
					conn.commit();
				else
					conn.rollback();
			}
			System.out.println("END:Transaction ended successfully [commit=" + commit + ']');
		} catch (SQLException e) {
			throw new CacheWriterException("ERROR:Failed to end transaction: " + cacheStoreSession.transaction(), e);
		}
	}
 
	private Connection openConnection(boolean autocommitFlag) throws SQLException {
		
		Connection objConn = DriverManager
				.getConnection("jdbc:mysql://localhost:3306/mysql_ignite?user=root2&password=atul");
		objConn.setAutoCommit(autocommitFlag);
		System.out.println("INFO:Connection object is created with autoCommitFlag as:" + autocommitFlag);
		return objConn;
	}
 
	private void endConnection(@Nullable Connection objConn) {
		if (!cacheStoreSession.isWithinTransaction() && objConn != null) {
			//Close connection as there is no transaction.
			try {
				objConn.close();
				System.out.println("INFO:Connection object is closed");
			} catch (SQLException ex) {
				ex.printStackTrace();
			}
		}
	}
 
	private Connection connection() throws SQLException {
		// Check if there is ongoing session; if so, we'll reuse it
		if (cacheStoreSession.isWithinTransaction()) {
			System.out.println("INFO:The cache store session is within Transaction");
			Map<Object, Object> connectionProperties = cacheStoreSession.properties();
			Connection conn = (Connection) connectionProperties.get(CONN_NAME);
			if (conn == null) {
				System.out.println("INFO:Connection does not exist; create a new connection with autoCommitFlag as False");
				conn = openConnection(false);
				connectionProperties.put(CONN_NAME, conn);
			}else{
				System.out.println("INFO:Connection exists; we'll reuse the same connection");
			}
			return conn;
		} else {
			System.out.println("INFO:The cache store session is NOT within Transaction; create a new connection");
			return openConnection(true);
		}
	}
}