package tech.aurora.bfadmin.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.annotation.Order;
import org.springframework.security.authentication.BadCredentialsException;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.annotation.web.configuration.WebSecurityConfigurerAdapter;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.web.authentication.preauth.AbstractPreAuthenticatedProcessingFilter;

import javax.servlet.http.HttpServletRequest;

@Configuration
@EnableWebSecurity
@Order(1)
public class SecurityConfig extends WebSecurityConfigurerAdapter {
    private static final String principalRequestHeader = "SecurityKey";

    //Unique security key used by UI, changes with every request
    public static String securityKey = "";

    @Value("${security.http.auth-token}")
    private String principalRequestValue;

    @Override
    protected void configure(HttpSecurity httpSecurity) throws Exception {
        ApiKeyAuthFilter filter = new ApiKeyAuthFilter(principalRequestHeader);
        filter.setAuthenticationManager(authentication -> {
            if (principalRequestValue.equals(authentication.getPrincipal().toString()) || securityKey.equals(authentication.getPrincipal().toString())) {
                authentication.setAuthenticated(true);
            } else {
                throw new BadCredentialsException("Permission denied.");
            }
            return authentication;
        });
        httpSecurity.
                antMatcher("/admin/**").
                csrf().disable().
                sessionManagement().sessionCreationPolicy(SessionCreationPolicy.STATELESS).
                and().addFilter(filter).authorizeRequests().anyRequest().authenticated();
    }

    private class ApiKeyAuthFilter extends AbstractPreAuthenticatedProcessingFilter {

        private String principalRequestHeader;

        public ApiKeyAuthFilter(String principalRequestHeader) {
            this.principalRequestHeader = principalRequestHeader;
        }

        @Override
        protected Object getPreAuthenticatedPrincipal(HttpServletRequest request) {
            return request.getHeader(principalRequestHeader);
        }

        @Override
        protected Object getPreAuthenticatedCredentials(HttpServletRequest request) {
            return "N/A";
        }
    }
}
