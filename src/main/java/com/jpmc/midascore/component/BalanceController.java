package com.jpmc.midascore.component;

import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Balance;
import com.jpmc.midascore.repository.UserRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;

@RestController
public class BalanceController {

    private static final Logger log = LoggerFactory.getLogger(BalanceController.class);

    @Autowired
    private UserRepository userRepository;

    @GetMapping("/balance")
    public Balance getBalance(@RequestParam long userId) {
        UserRecord user = userRepository.findById(userId).orElse(null);
        if (user == null) {
            log.warn("Balance query for unknown userId={} — returning zero", userId);
            return new Balance(BigDecimal.ZERO);
        }
        log.info("Balance query — userId={} name={} balance={}",
            userId, user.getName(), user.getBalance());
        return new Balance(user.getBalance());
    }
}

