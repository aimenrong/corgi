/**
 * 
 */
package corgi.hub.core.http.controller;

import corgi.hub.core.auth.service.LoginProxyService;
import corgi.hub.core.bean.Account;
import corgi.hub.core.bean.Principal;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author a
 *
 */
@RestController
public class LoginController {
    @Autowired
    protected LoginProxyService loginProxyService;

    public LoginController() {
        System.out.println(String.format("%s init success", this.getClass().getName()));
    }

    @RequestMapping(value = "/accounts/login", method = RequestMethod.POST, consumes = "application/json")
    public Principal login(@RequestBody Account account) {
        Principal principal = loginProxyService.login(account);
        return principal;
    }
}
