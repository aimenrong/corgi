package corgi.hub.core.auth.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;
import org.springframework.cloud.client.loadbalancer.LoadBalanced;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import corgi.hub.core.bean.Account;
import corgi.hub.core.bean.Principal;

/**
 * Created by Terry LIANG on 12/15/2016.
 */

@EnableDiscoveryClient
@Service
public class LoginProxyService {
    @Autowired
    @LoadBalanced
    protected RestTemplate restTemplate;

    @Value("${app.account.service.url}")
    protected String accountServiceUrl;

    @Value("${app.id.service.url}")
    protected String idServiceUrl;

    public LoginProxyService() {
        System.out.println(String.format("%s init success", this.getClass().getName()));
    }

    public Principal login(Account account) {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity request = new HttpEntity(account, headers);
        Principal principal = restTemplate.postForObject(accountServiceUrl + "/accounts/login", request, Principal.class);

        return  principal;
    }

}
