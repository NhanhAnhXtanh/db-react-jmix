package com.company.tachconnectjmix.metadata.dto;

import com.company.tachconnectjmix.metadata.enums.OrderType;

public class QueryOrderParam {

    private String key;
    private String orderType;

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public OrderType getOrderType() {
        return OrderType.fromId(orderType);
    }

    public void setOrderType(String orderType) {
        this.orderType = orderType;
    }
}
