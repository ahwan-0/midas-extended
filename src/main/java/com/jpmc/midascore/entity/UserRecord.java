package com.jpmc.midascore.entity;

import jakarta.persistence.*;

@Entity
public class UserRecord {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private long id;

    private String name;
    private float balance;

    public UserRecord() {}

    public UserRecord(String name, float balance) {
        this.name = name;
        this.balance = balance;
    }

    public long getId() { return id; }

    public String getName() { return name; }
    public void setName(String name) { this.name = name; }

    public float getBalance() { return balance; }
    public void setBalance(float balance) { this.balance = balance; }
}
