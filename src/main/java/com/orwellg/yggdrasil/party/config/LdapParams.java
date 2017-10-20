package com.orwellg.yggdrasil.party.config;

import com.netflix.config.DynamicStringProperty;

public class LdapParams {
	// AWS test environment URL: "ldap://ec2-35-176-201-54.eu-west-2.compute.amazonaws.com:389"
	public static final String URL_DEFAULT = "ldap://ec2-35-176-201-54.eu-west-2.compute.amazonaws.com:389";
	public static final String ADMIN_DN_DEFAULT = "cn=admin,dc=ec2-35-176-201-54,dc=eu-west-2,dc=compute,dc=amazonaws,dc=com";
	public static final String ADMIN_PWD_DEFAULT = "Password123$";
	public static final String USERS_GROUP_DN_DEFAULT = "ou=Users,dc=ec2-35-176-201-54,dc=eu-west-2,dc=compute,dc=amazonaws,dc=com";

	// Store DynamicXxxProperty, so that .get() is invoked directly in the ldapParams getters, so that the changes can take effect dynamically:
	protected DynamicStringProperty url;
	protected DynamicStringProperty adminDn;
	protected DynamicStringProperty adminPwd;
	protected DynamicStringProperty usersGroupDn;

	/**
	 * Empty constructor. Set properties after creation.
	 */
	public LdapParams() {
	}
	
	/**
	 * Full constructor.
	 * @param url
	 * @param adminDn
	 * @param adminPwd
	 * @param usersGroupDn
	 */
	public LdapParams(DynamicStringProperty url, DynamicStringProperty adminDn, DynamicStringProperty adminPwd,
			DynamicStringProperty usersGroupDn) {
		super();
		this.url = url;
		this.adminDn = adminDn;
		this.adminPwd = adminPwd;
		this.usersGroupDn = usersGroupDn;
	}



	public String getAdminDn() {
		return (adminDn != null ? adminDn.get() : ADMIN_DN_DEFAULT);
	}

	public String getAdminPwd() {
		return (adminPwd != null ? adminPwd.get() : ADMIN_PWD_DEFAULT);
	}

	public String getUrl() {
		return (url != null ? url.get() : URL_DEFAULT);
	}

	public void setAdminDn(DynamicStringProperty adminDn) {
		this.adminDn = adminDn;
	}

	public void setAdminPwd(DynamicStringProperty adminPwd) {
		this.adminPwd = adminPwd;
	}

	public void setUrl(DynamicStringProperty url) {
		this.url = url;
	}

	public String getUsersGroupDn() {
		return (usersGroupDn != null ? usersGroupDn.get() : USERS_GROUP_DN_DEFAULT);
	}

	public void setUsersGroupDn(DynamicStringProperty usersGroupDn) {
		this.usersGroupDn = usersGroupDn;
	}
}
