package com.jpmc.midascore.component;

import com.jpmc.midascore.foundation.Incentive;
import com.jpmc.midascore.foundation.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

@Component
public class IncentiveService {

    private static final Logger log = LoggerFactory.getLogger(IncentiveService.class);
    private final RestTemplate restTemplate = new RestTemplate();

    public Incentive getIncentive(Transaction transaction) {
        try {
            Incentive incentive = restTemplate.postForObject(
                "http://localhost:8080/incentive",
                transaction,
                Incentive.class
            );
            return incentive != null ? incentive : new Incentive();
        } catch (Exception e) {
            log.warn("Incentive API unavailable for senderId={} — defaulting to zero. Reason: {}",
                transaction.getSenderId(), e.getMessage());
            return new Incentive();
        }
    }
}

