package com.jpmc.midascore.foundation;

public class Balance {
    private float amount;

    public Balance() {}

    public Balance(float amount) {
        this.amount = amount;
    }

    public float getAmount() { return amount; }
    public void setAmount(float amount) { this.amount = amount; }

    @Override
    public String toString() {
        return String.valueOf(amount);
    }
}


