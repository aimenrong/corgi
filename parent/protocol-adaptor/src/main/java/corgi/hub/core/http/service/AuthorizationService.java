package corgi.hub.core.http.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.client.loadbalancer.LoadBalanced;
import org.springframework.http.*;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

/**
 * Created by Terry LIANG on 2016/12/18.
 */
@Service
public class AuthorizationService {
    @Autowired
    @LoadBalanced
    protected RestTemplate restTemplate;

    @Value("${app.account.service.url}")
    protected String accountServiceUrl;

    public AuthorizationService() {
        System.out.println(String.format("%s init success", this.getClass().getName()));
    }

    public boolean validAndRefreshToken(String clientId, String tokenId) {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity request = new HttpEntity(null, headers);
        ResponseEntity<Boolean> resp = restTemplate.exchange(accountServiceUrl + "/accounts/validAndRefreshToken/" + clientId + "/" + tokenId, HttpMethod.PUT,request,  Boolean.class);
        return resp.getBody().booleanValue();
    }
}
