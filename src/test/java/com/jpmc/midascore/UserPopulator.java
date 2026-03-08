package com.jpmc.midascore;
import java.math.BigDecimal;
import com.jpmc.midascore.component.DatabaseConduit;
import com.jpmc.midascore.entity.UserRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class UserPopulator {
    @Autowired
    private FileLoader fileLoader;

    @Autowired
    private DatabaseConduit databaseConduit;

    public void populate() {
        String[] userLines = fileLoader.loadStrings("/test_data/lkjhgfdsa.hjkl");
        for (String userLine : userLines) {
            String[] userData = userLine.split(", ");
        UserRecord user = new UserRecord(userData[0], BigDecimal.valueOf(Float.parseFloat(userData[1])));
            databaseConduit.save(user);
        }
    }
}
