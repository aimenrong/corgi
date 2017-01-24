/**
 * 
 */
package corgi.hub.core.bean;

import java.util.Date;

/**
 * @author a
 *
 */
public class Principal {
	private String tokenId;
	
	private Date tokenExpireDate;
	
	private String username;

	public String getTokenId() {
		return tokenId;
	}

	public void setTokenId(String tokenId) {
		this.tokenId = tokenId;
	}

	public Date getTokenExpireDate() {
		return tokenExpireDate;
	}

	public void setTokenExpireDate(Date tokenExpireDate) {
		this.tokenExpireDate = tokenExpireDate;
	}

	public String getUsername() {
		return username;
	}

	public void setUsername(String username) {
		this.username = username;
	}
}
