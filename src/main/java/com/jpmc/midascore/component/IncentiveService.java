package com.jpmc.midascore.component;

import com.jpmc.midascore.foundation.Incentive;
import com.jpmc.midascore.foundation.Transaction;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

@Component
public class IncentiveService {

    private final RestTemplate restTemplate = new RestTemplate();

    public Incentive getIncentive(Transaction transaction) {
        try {
            return restTemplate.postForObject(
                "http://localhost:8080/incentive",
                transaction,
                Incentive.class
            );
        } catch (Exception e) {
            return new Incentive();
        }
    }
}

