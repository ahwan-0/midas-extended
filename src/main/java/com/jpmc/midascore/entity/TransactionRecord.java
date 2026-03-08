package com.jpmc.midascore.entity;

import jakarta.persistence.*;
import java.math.BigDecimal;
import java.time.LocalDateTime;

@Entity
public class TransactionRecord {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private long id;

    @ManyToOne
    private UserRecord sender;

    @ManyToOne
    private UserRecord recipient;

    @Column(precision = 19, scale = 4)
    private BigDecimal amount;

    @Column(precision = 19, scale = 4)
    private BigDecimal incentive;

    private LocalDateTime timestamp;

    public TransactionRecord() {}

    public TransactionRecord(UserRecord sender, UserRecord recipient,
                             BigDecimal amount, BigDecimal incentive) {
        this.sender = sender;
        this.recipient = recipient;
        this.amount = amount;
        this.incentive = incentive;
        this.timestamp = LocalDateTime.now();
    }
}


