package com.jpmc.midascore.foundation;

import java.math.BigDecimal;

public class Balance {

    private BigDecimal amount;

    public Balance() {
        this.amount = BigDecimal.ZERO;
    }

    public Balance(BigDecimal amount) {
        this.amount = amount;
    }

    public BigDecimal getAmount() { return amount; }
    public void setAmount(BigDecimal amount) { this.amount = amount; }

    @Override
    public String toString() { return String.valueOf(amount); }
}


