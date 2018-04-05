CREATE TABLE finished_url(id SERIAL, url VARCHAR UNIQUE, type VARCHAR);
CREATE TABLE baike_html(id SERIAL, html BYTEA, url VARCHAR UNIQUE, type VARCHAR);
CREATE TABLE baike_knowledge(id SERIAL, url VARCHAR UNIQUE, knowledge JSON, type VARCHAR);
CREATE TABLE url_label(id SERIAL, url VARCHAR UNIQUE, label JSON, type VARCHAR);
CREATE TABLE manual_label(id SERIAL,
    url VARCHAR UNIQUE,
    count INT,
    ratio FLOAT,
    label JSON,
    flag VARCHAR,
    CHECK (flag IN ('auto', 'human')),
    knowledge JSON,
    type VARCHAR);
CREATE TABLE mention2entity(id SERIAL, mention VARCHAR, title VARCHAR, url VARCHAR, UNIQUE(mention, title, url))