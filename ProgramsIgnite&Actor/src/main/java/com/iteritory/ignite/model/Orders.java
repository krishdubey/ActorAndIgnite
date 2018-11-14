package com.iteritory.ignite.model;

import java.sql.Date;
public class Orders {

	int orderNumber;
	String orderType;
	Date orderFulfillmentDate;
	OrderLines orderLine[];
	
	public Orders(int orderNumber, String orderType, Date orderFulfillmentDate, OrderLines[] orderLine) 
	{
		super();
		this.orderNumber = orderNumber;
		this.orderType = orderType;
		this.orderFulfillmentDate = orderFulfillmentDate;
		this.orderLine = orderLine;
	}
	public Orders(int orderNumber, String orderType, Date orderFulfillmentDate) {
		// TODO Auto-generated constructor stub
		super();
		this.orderNumber = orderNumber;
		this.orderType = orderType;
		this.orderFulfillmentDate = orderFulfillmentDate;
	}
	public int getOrderNumber() {
		return orderNumber;
	}
	public void setOrderNumber(int orderNumber) {
		this.orderNumber = orderNumber;
	}
	public String getOrderType() {
		return orderType;
	}
	public void setOrderType(String orderType) {
		this.orderType = orderType;
	}
	public Date getOrderFulfillmentDate() {
		return orderFulfillmentDate;
	}
	public void setOrderFulfillmentDate(Date orderFulfillmentDate) {
		this.orderFulfillmentDate = orderFulfillmentDate;
	}
	public OrderLines[] getOrderLine() {
		return orderLine;
	}
	public void setOrderLine(OrderLines[] orderLine) {
		this.orderLine = orderLine;
	}
}
