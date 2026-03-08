package com.jpmc.midascore.entity;

import jakarta.persistence.*;
import java.math.BigDecimal;

@Entity
public class UserRecord {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private long id;

    private String name;

    @Column(precision = 19, scale = 4)
    private BigDecimal balance;

    public UserRecord() {}

    public UserRecord(String name, BigDecimal balance) {
        this.name = name;
        this.balance = balance;
    }

    public long getId() { return id; }
    public String getName() { return name; }
    public void setName(String name) { this.name = name; }
    public BigDecimal getBalance() { return balance; }
    public void setBalance(BigDecimal balance) { this.balance = balance; }
}
