package com.orwellg.yggdrasil.party.ldap;

import java.util.Hashtable;

import javax.naming.Context;
import javax.naming.NamingException;
import javax.naming.directory.Attribute;
import javax.naming.directory.Attributes;
import javax.naming.directory.BasicAttribute;
import javax.naming.directory.BasicAttributes;
import javax.naming.directory.DirContext;
import javax.naming.directory.InitialDirContext;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.orwellg.umbrella.commons.types.party.Party;
import com.orwellg.yggdrasil.party.config.LdapParams;

/**
 * Utility to connect to LDAP.<br/>
 * Needs to receive LDAP parameters from ConfigurationParams (in ComponentFactory).
 * See LdapParams.
 * 
 * @author c.friaszapater
 *
 */
public class LdapUtil {
	
	protected static final String CTX_FACTORY = "com.sun.jndi.ldap.LdapCtxFactory";
	protected static final String CONNTYPE = "simple";

	// XXX This could be changed to be an interface, so that LdapUtil class is not party-storm dependent
	protected LdapParams ldapParams;

	private final static Logger LOG = LogManager.getLogger(LdapUtil.class);

	public LdapUtil(LdapParams ldapParams) {
		this.ldapParams = ldapParams;
	}
	
	/**
	 * The userId is stored in the cn subattribute of the attribute “uid”. Does not
	 * set uidNumber attribute.
	 * 
	 * @param userId
	 *            db unique user id
	 * @param username
	 * @param password
	 * @return
	 * @throws NamingException
	 */
	public DirContext addUser(String userId, String username, String password) throws NamingException {
		DirContext dctx = null;
		try {
			dctx = connectLdap();

			// Create a container set of attributes
			final Attributes container = new BasicAttributes();

			// Create the objectclass to add
			final Attribute objClasses = new BasicAttribute("objectClass");
			objClasses.add("inetOrgPerson");

			// Assign the username, first name, and last name
			final Attribute commonName = new BasicAttribute("cn", username);
			final Attribute email = new BasicAttribute("mail", username);
			final Attribute givenName = new BasicAttribute("givenName", username);
			String uidVal = getUserDN(userId);
			final Attribute uid = new BasicAttribute("uid", uidVal);
			// This raises "InvalidAttributeValueException: Malformed 'uidNumber' attribute
			// value":
			// final Attribute uidNumber = new BasicAttribute("uidNumber", userId);
			final Attribute surName = new BasicAttribute("sn", username);

			// Add password
			final Attribute userPassword = new BasicAttribute("userpassword", password);

			// Add these to the container
			container.put(objClasses);
			container.put(commonName);
			container.put(givenName);
			container.put(email);
			container.put(uid);
			// container.put(uidNumber);
			container.put(surName);
			container.put(userPassword);

			// Create the entry
			DirContext createdSubcontext = dctx.createSubcontext(uidVal, container);
			return createdSubcontext;
		} finally {
			if (null != dctx) {
				try {
					dctx.close();
				} catch (final NamingException e) {
					System.out.println("Error in closing ldap " + e);
				}
			}
		}
	}

	/**
	 * @param userId
	 *            db unique user id
	 * @return user retrieved from ldap.
	 */
	public DirContext getUserById(String userId) throws NamingException {
		DirContext dctx = null;
		try {
			dctx = connectLdap();

			DirContext user = (DirContext) dctx.lookup(getUserDN(userId));
			return user;

		} finally {
			if (null != dctx) {
				try {
					dctx.close();
				} catch (final NamingException e) {
					System.out.println("Error in closing ldap " + e);
				}
			}
		}
	}

	/**
	 * Remove user in ldap.
	 * 
	 * @param userId
	 * @throws NamingException
	 */
	public void removeUser(String userId) throws NamingException {
		DirContext dctx = null;
		try {
			dctx = connectLdap();

			dctx.destroySubcontext(getUserDN(userId));

		} finally {
			if (null != dctx) {
				try {
					dctx.close();
				} catch (final NamingException e) {
					System.out.println("Error in closing ldap " + e);
				}
			}
		}
	}

	protected DirContext connectLdap() throws NamingException {
		DirContext dctx;
		LOG.info("Connecting to LDAP at {} ...", ldapParams.getUrl());

		final Hashtable<Object, Object> env = new Hashtable<Object, Object>();
		env.put(Context.INITIAL_CONTEXT_FACTORY, CTX_FACTORY);
		env.put(Context.PROVIDER_URL, ldapParams.getUrl());
		env.put(Context.SECURITY_AUTHENTICATION, CONNTYPE);
		env.put(Context.SECURITY_PRINCIPAL, ldapParams.getAdminDn());
		env.put(Context.SECURITY_CREDENTIALS, ldapParams.getAdminPwd());
		dctx = new InitialDirContext(env);
		LOG.info("...LDAP connected at {} .", ldapParams.getUrl());
		return dctx;
	}

	protected String getUserDN(String userId) {
		// cn=Username,ou=Users,dc=ec2-35-176-201-54,dc=eu-west-2,dc=compute,dc=amazonaws,dc=com
		String userDN = new StringBuffer().append("cn=").append(userId).append("," + ldapParams.getUsersGroupDn())
				.toString();
		LOG.debug(userDN);
		return userDN;
	}

	public void registerLdapParty(Party p) throws NamingException, Exception {
		String userId = p.getParty().getId().getId();
		// Only register in LDAP if there is username and password
		String username = p.getParty().getUsername();
		String password = p.getParty().getPassword();

		if (username != null && password != null) {
			addUser(userId, username.toString(), password.toString());
		} else {
			// If there are no username and no password, silently continue (no error)
			if (username == null && password == null) {
				LOG.info(
						"Username and password for received Party are null, so there will be no LDAP registration.");
			} else {
				// If there is only one of them, raise error
				throw new Exception(String.format(
						"Need to provide username and password or none of them. Provided username = %s, password = %s",
						username, (password == null ? "null" : "not null")));
			}
		}
	}
}
