package com.jpmc.midascore.component;

import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Incentive;
import com.jpmc.midascore.foundation.Transaction;
import com.jpmc.midascore.repository.TransactionRepository;
import com.jpmc.midascore.repository.UserRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;

@Component
public class DatabaseConduit {

    private static final Logger log = LoggerFactory.getLogger(DatabaseConduit.class);

    @Autowired private UserRepository userRepository;
    @Autowired private TransactionRepository transactionRepository;
    @Autowired private IncentiveService incentiveService;

    public void save(UserRecord userRecord) {
        userRepository.save(userRecord);
    }

    @Transactional
    public void process(Transaction transaction) {
        UserRecord sender = userRepository.findById(transaction.getSenderId()).orElse(null);
        UserRecord recipient = userRepository.findById(transaction.getRecipientId()).orElse(null);

        if (sender == null || recipient == null) {
            log.warn("Transaction rejected — invalid sender ({}) or recipient ({})",
                transaction.getSenderId(), transaction.getRecipientId());
            return;
        }

        BigDecimal amount = BigDecimal.valueOf(transaction.getAmount());

        if (sender.getBalance().compareTo(amount) < 0) {
            log.warn("Transaction rejected — insufficient balance. sender={} balance={} required={}",
                sender.getName(), sender.getBalance(), amount);
            return;
        }

        Incentive incentive = incentiveService.getIncentive(transaction);
        BigDecimal incentiveAmount = incentive.getAmount();

        sender.setBalance(sender.getBalance().subtract(amount));
        recipient.setBalance(recipient.getBalance().add(amount).add(incentiveAmount));

        userRepository.save(sender);
        userRepository.save(recipient);
        transactionRepository.save(new TransactionRecord(sender, recipient, amount, incentiveAmount));

        log.info("Transaction processed — sender={} recipient={} amount={} incentive={}",
            sender.getName(), recipient.getName(), amount, incentiveAmount);
    }
}
