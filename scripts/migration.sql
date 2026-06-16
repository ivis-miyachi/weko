-- Active: 1734410072859@@172.18.0.1@25401
-- カラム追加時に初期値で設定が必要
-- 既存データへのsystem_created設定
-- oaiserver_set
ALTER TABLE oaiserver_set ADD COLUMN system_created BOOLEAN;
UPDATE oaiserver_set SET system_created = TRUE;
ALTER TABLE oaiserver_set ALTER COLUMN system_created SET NOT NULL;

-- stats_eventsのtypeカラムのnot null制約解除
ALTER TABLE stats_events ALTER COLUMN type DROP NOT NULL;

-- stats_aggregationsのtypeカラムのnot null制約解除
ALTER TABLE stats_aggregation ALTER COLUMN type DROP NOT NULL;

-- stats_bookmarkのtypeカラムのnot null制約解除
ALTER TABLE stats_bookmark ALTER COLUMN type DROP NOT NULL;


-- pidrelations_pidrelation
-- 既存のプライマリキー削除
ALTER TABLE pidrelations_pidrelation DROP CONSTRAINT pk_pidrelations_pidrelation;
-- 新しいプライマリキーを設定
ALTER TABLE pidrelations_pidrelation ADD CONSTRAINT pk_pidrelations_pidrelation PRIMARY KEY (parent_id, child_id, relation_type);


ALTER TABLE access_actionsroles
DROP CONSTRAINT IF EXISTS fk_access_actionsroles_role_id_accounts_role;

ALTER TABLE accounts_userrole
DROP CONSTRAINT IF EXISTS fk_accounts_userrole_role_id;

ALTER TABLE communities_community
DROP CONSTRAINT fk_communities_community_id_role_accounts_role;

ALTER TABLE shibboleth_userrole
DROP CONSTRAINT IF EXISTS fk_shibboleth_userrole_role_id;

ALTER TABLE workflow_flow_action_role
DROP CONSTRAINT IF EXISTS fk_workflow_flow_action_role_action_role_accounts_role;

ALTER TABLE workflow_userrole
DROP CONSTRAINT IF EXISTS fk_workflow_userrole_role_id_accounts_role;


ALTER TABLE access_actionsroles
ALTER COLUMN role_id TYPE VARCHAR(80);

ALTER TABLE accounts_userrole
ALTER COLUMN role_id TYPE VARCHAR(80);

ALTER TABLE communities_community
ALTER COLUMN id_role TYPE VARCHAR(80);

ALTER TABLE shibboleth_userrole
ALTER COLUMN role_id TYPE VARCHAR(80);

ALTER TABLE workflow_flow_action_role
ALTER COLUMN action_role TYPE VARCHAR(80);

ALTER TABLE workflow_userrole
ALTER COLUMN role_id TYPE VARCHAR(80);

ALTER TABLE accounts_role
ALTER COLUMN id TYPE VARCHAR(80);


ALTER TABLE access_actionsroles
ADD CONSTRAINT fk_access_actionsroles_role_id_accounts_role
FOREIGN KEY (role_id) REFERENCES accounts_role(id);

ALTER TABLE accounts_userrole
ADD CONSTRAINT fk_accounts_userrole_role_id
FOREIGN KEY (role_id) REFERENCES accounts_role(id);

ALTER TABLE communities_community
ADD CONSTRAINT fk_communities_community_id_role_accounts_role
FOREIGN KEY (id_role) REFERENCES accounts_role(id);

ALTER TABLE shibboleth_userrole
ADD CONSTRAINT fk_shibboleth_userrole_role_id
FOREIGN KEY (role_id) REFERENCES accounts_role(id);

ALTER TABLE workflow_flow_action_role
ADD CONSTRAINT fk_workflow_flow_action_role_action_role_accounts_role
FOREIGN KEY (action_role) REFERENCES accounts_role(id);

ALTER TABLE workflow_userrole
ADD CONSTRAINT fk_workflow_userrole_role_id_accounts_role
FOREIGN KEY (role_id) REFERENCES accounts_role(id);

-- accounts_role
ALTER TABLE accounts_role ADD COLUMN created TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL;

ALTER TABLE accounts_role ADD COLUMN updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL;

ALTER TABLE accounts_role ADD COLUMN is_managed BOOLEAN DEFAULT TRUE NOT NULL;

ALTER TABLE accounts_role ADD COLUMN version_id INTEGER;
UPDATE accounts_role SET version_id = 0;
ALTER TABLE accounts_role ALTER COLUMN version_id SET NOT NULL;

-- accounts_user
ALTER TABLE accounts_user ADD COLUMN created TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL;

ALTER TABLE accounts_user ADD COLUMN updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL;

ALTER TABLE accounts_user
ADD COLUMN username VARCHAR(255) UNIQUE,
ADD COLUMN displayname VARCHAR(255);

ALTER TABLE accounts_user
ADD COLUMN "domain" VARCHAR(255);


ALTER TABLE accounts_user ADD COLUMN version_id INTEGER;
UPDATE accounts_user SET version_id = 0;
ALTER TABLE accounts_user ALTER COLUMN version_id SET NOT NULL;


ALTER TABLE accounts_user
ADD COLUMN profile JSON DEFAULT '{}'::json,
ADD COLUMN preferences JSON DEFAULT '{}'::json;

ALTER TABLE accounts_user
ADD COLUMN blocked_at TIMESTAMP,
ADD COLUMN verified_at TIMESTAMP;


CREATE TABLE accounts_user_login_information (
    user_id INTEGER PRIMARY KEY,
    last_login_at TIMESTAMP,
    current_login_at TIMESTAMP,
    last_login_ip character varying(50),
    current_login_ip character varying(50),
    login_count INTEGER,
    CONSTRAINT fk_accounts_login_information_user_id FOREIGN KEY (user_id) REFERENCES accounts_user(id)
);
-- 
CREATE TABLE accounts_useridentity (
    created TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
    id VARCHAR(255) NOT NULL,
    method VARCHAR(255) NOT NULL,
    id_user INTEGER NOT NULL,
    PRIMARY KEY (id, method),
    FOREIGN KEY (id_user) REFERENCES accounts_user(id)
);

CREATE INDEX accounts_useridentity_id_user_method ON accounts_useridentity (id, method);

-- accounts_useridentity
-- ←oauthclient_useridentity_bk
insert into accounts_useridentity
(
    created
    ,updated
    ,id
    ,method
    ,id_user
)
select
    created
    ,updated
    ,id
    ,method
    ,id_user
from
    oauthclient_useridentity_bk;
    
-- accounts_user_login_information
-- ←accounts_user_bk
insert into accounts_user_login_information
(
    user_id
    ,last_login_at
    ,current_login_at
    ,last_login_ip
    ,current_login_ip
    ,login_count
)
select
    id
    ,last_login_at
    ,current_login_at
    ,last_login_ip
    ,current_login_ip
    ,login_count
from
    accounts_user_bk;
    
-- accounts_userテーブルからカラム削除

ALTER TABLE accounts_user
DROP COLUMN last_login_at,
DROP COLUMN current_login_at,
DROP COLUMN last_login_ip,
DROP COLUMN current_login_ip,
DROP COLUMN login_count;



-- テーブル作成
CREATE TABLE accounts_domain_org (
    id SERIAL PRIMARY KEY,
    pid VARCHAR(255) UNIQUE,
    name VARCHAR(255) NOT NULL,
    json JSONB DEFAULT '{}'::jsonb NOT NULL,
    parent_id INTEGER,
    CONSTRAINT fk_accounts_domain_org_parent_id FOREIGN KEY (parent_id) REFERENCES accounts_domain_org(id)
);

CREATE TABLE accounts_domain_category (
    id SERIAL PRIMARY KEY,
    label VARCHAR(255)
);

CREATE TABLE accounts_domains (
    created TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
    id SERIAL PRIMARY KEY,
    domain VARCHAR(255) UNIQUE NOT NULL,
    tld VARCHAR(255) NOT NULL,
    status INTEGER DEFAULT 1 NOT NULL,
    flagged BOOLEAN DEFAULT FALSE NOT NULL,
    flagged_source VARCHAR(255) DEFAULT '' NOT NULL,
    org_id INTEGER,
    category INTEGER,
    num_users INTEGER DEFAULT 0 NOT NULL,
    num_active INTEGER DEFAULT 0 NOT NULL,
    num_inactive INTEGER DEFAULT 0 NOT NULL,
    num_confirmed INTEGER DEFAULT 0 NOT NULL,
    num_verified INTEGER DEFAULT 0 NOT NULL,
    num_blocked INTEGER DEFAULT 0 NOT NULL,
    FOREIGN KEY (org_id) REFERENCES accounts_domain_org(id),
    FOREIGN KEY (category) REFERENCES accounts_domain_category(id)
);

-- テーブルバックアップの削除
drop table oaiserver_set_bk cascade;
  
drop table pidrelations_pidrelation_bk cascade; 

drop table access_actionsroles_bk cascade; 

drop table accounts_userrole_bk cascade; 

drop table accounts_role_bk cascade; 

drop table accounts_user_bk cascade; 

drop table oauthclient_useridentity_bk  cascade; 


