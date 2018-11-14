package com.iteritory.ignite.model;

public class OrderLines {

	int orderNumber;
	int orderLineNumber;
	String itemName;
	int itemQty;
	
	public OrderLines() {
		
	}
	public OrderLines(int orderNumber, int orderLineNumber, String itemName, int itemQty) {
		super();
		this.orderNumber = orderNumber;
		this.orderLineNumber = orderLineNumber;
		this.itemName = itemName;
		this.itemQty = itemQty;
	}
	
	public int getOrderNumber()
	{
		return orderNumber;
	}
	
	public void setOrderNumber(int orderNumber)
	{
		this.orderNumber = orderNumber;
	}
	
	public int getOrderLineNumber()
	{
		return orderLineNumber;
	}
	
	public void setOrderLineNumber(int orderLineNumber) {
		this.orderLineNumber = orderLineNumber;
	}
	public String getItemName() {
		return itemName;
	}
 
	public void setItemName(String itemName) {
		this.itemName = itemName;
	}
 
	public int getItemQty() {
		return itemQty;
	}
 
	public void setItemQty(int itemQty) {
		this.itemQty = itemQty;
	}
	
	
}
