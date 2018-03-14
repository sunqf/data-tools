CREATE TABLE finished_url(id SERIAL, url VARCHAR UNIQUE, type VARCHAR);
CREATE TABLE baike_html(id SERIAL, html VARCHAR, url VARCHAR UNIQUE, type VARCHAR);
CREATE TABLE baike_knowledge(id SERIAL, url VARCHAR UNIQUE, knowledge JSON, type VARCHAR)