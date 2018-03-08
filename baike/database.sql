CREATE TABLE finished_url(id SERIAL, keyword VARCHAR, type VARCHAR);
CREATE TABLE baike_html(id SERIAL, html VARCHAR, url VARCHAR, type VARCHAR);

CREATE TABLE baike_knowledge(id SERIAL, url VARCHAR, knowledge VARCHAR, type VARCHAR)