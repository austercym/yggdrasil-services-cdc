DROP TABLE IF EXISTS Party;

CREATE TABLE Party (
  Party_ID varchar(30) NOT NULL,
  FirstName varchar(255) DEFAULT NULL,
  LastName varchar(255) DEFAULT NULL,
  BusinessName varchar(255) DEFAULT NULL,
  TradingName varchar(255) DEFAULT NULL,
  Title int(255) DEFAULT NULL,
  Salutation varchar(255) DEFAULT NULL,
  OtherNames varchar(128) DEFAULT NULL,
  ID_List text,
  Addresses text,
  Party_InfoAssets text,
  PRIMARY KEY (Party_ID),
  UNIQUE KEY unique_Party__ID (Party_ID)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE PartyNonPersonalDetails (
  NPP_ID varchar(30) NOT NULL,
  Party_ID varchar(30) NOT NULL,
  SICCodes text,
  BusinessRegistrationCountry varchar(255) DEFAULT NULL,
  BusinessRegistrationDate date DEFAULT NULL,
  TradingStartDate date DEFAULT NULL,
  IncorporationDetail text,
  OtherAddresses text,
  Website varchar(255) DEFAULT NULL,
  NPPContactDetails text,
  StockMarket text,
  Turnover text,
  PRIMARY KEY (NPP_ID),
  KEY lnk_Party_PartyNonPersonalDetails (Party_ID),
  CONSTRAINT lnk_Party_PartyNonPersonalDetails FOREIGN KEY (Party_ID) REFERENCES Party (Party_ID) ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE PartyPersonalDetails (
  PP_ID varchar(30) NOT NULL,
  Party_ID varchar(30) NOT NULL,
  EmploymentDetails text DEFAULT NULL,
  Citizenships text DEFAULT NULL,
  TaxResidency text DEFAULT NULL,
  Email text DEFAULT NULL,
  Telephone text DEFAULT NULL,
  StaffIndicator tinyint(1) DEFAULT NULL,
  Staff text DEFAULT NULL,
  Gender int(255) DEFAULT NULL,
  DateOfBirth datetime DEFAULT NULL,
  Nationality varchar(255),
  Tags text DEFAULT NULL,
  PRIMARY KEY (PP_ID),
  UNIQUE KEY unique_PP_ID (PP_ID),
  KEY lnk_Party_PartyPersonalDetails (Party_ID),
  CONSTRAINT lnk_Party_PartyPersonalDetails FOREIGN KEY (Party_ID) REFERENCES Party (Party_ID) ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
